package com.hotkey.proxy.cache;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hotkey.proxy.config.ProxyProperties;
import com.hotkey.proxy.config.RuntimeSettings;
import com.hotkey.proxy.hash.ConsistentHashRing;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class RedisCacheClient implements ResponseCache {

    private static final Logger log = LoggerFactory.getLogger(RedisCacheClient.class);
    private static final int MAX_BODY_BYTES = 512_000;
    private static final SecureRandom TOKENS = new SecureRandom();
    /**
     * Keeps a generation token available for an in-flight conditional store.
     * When the token expires or is evicted, the store is refused. This retention is not a bound on how long a cached body may be served.
     */
    private static final long GENERATION_RETENTION_MS = 300_000;

    private static final String LOOKUP_SCRIPT = """
            local keyType = redis.call('TYPE', KEYS[1])['ok']
            if keyType == 'string' then
              return redis.call('GET', KEYS[1])
            end
            if keyType == 'hash' then
              local body = redis.call('HGET', KEYS[1], 'body')
              if body == false then
                return nil
              end
              return body
            end
            return nil
            """;

    private static final String PIN_SCRIPT = """
            local candidate = ARGV[1]
            local retention = ARGV[2]
            if candidate == nil or candidate == '' then
              return nil
            end
            local keyType = redis.call('TYPE', KEYS[1])['ok']
            if keyType == 'none' then
              redis.call('HSET', KEYS[1], 'gen', candidate)
              redis.call('PEXPIRE', KEYS[1], retention)
              return candidate
            end
            if keyType == 'string' then
              local body = redis.call('GET', KEYS[1])
              redis.call('DEL', KEYS[1])
              redis.call('HSET', KEYS[1], 'gen', candidate, 'body', body)
              redis.call('PEXPIRE', KEYS[1], retention)
              return candidate
            end
            if keyType == 'hash' then
              local gen = redis.call('HGET', KEYS[1], 'gen')
              if gen == false or gen == '' then
                redis.call('HSET', KEYS[1], 'gen', candidate)
                redis.call('PEXPIRE', KEYS[1], retention)
                return candidate
              end
              redis.call('PEXPIRE', KEYS[1], retention)
              return gen
            end
            return nil
            """;

    private static final String STORE_SCRIPT = """
            local expected = ARGV[1]
            local body = ARGV[2]
            local ttl = ARGV[3]
            if redis.call('TYPE', KEYS[1])['ok'] ~= 'hash' then
              return 0
            end
            local gen = redis.call('HGET', KEYS[1], 'gen')
            if gen == false or gen ~= expected then
              return 0
            end
            redis.call('HSET', KEYS[1], 'body', body)
            redis.call('PEXPIRE', KEYS[1], ttl)
            return 1
            """;

    private static final String INVALIDATE_SCRIPT = """
            local fresh = ARGV[1]
            local retention = ARGV[2]
            if fresh == nil or fresh == '' then
              return 0
            end
            local keyType = redis.call('TYPE', KEYS[1])['ok']
            if keyType == 'string' or keyType == 'none' then
              redis.call('DEL', KEYS[1])
              redis.call('HSET', KEYS[1], 'gen', fresh)
              redis.call('PEXPIRE', KEYS[1], retention)
              return 1
            end
            if keyType == 'hash' then
              redis.call('HSET', KEYS[1], 'gen', fresh)
              redis.call('HDEL', KEYS[1], 'body')
              redis.call('PEXPIRE', KEYS[1], retention)
              return 1
            end
            return 0
            """;

    private final RuntimeSettings settings;
    private final ObjectMapper mapper;
    private final List<RedisNode> nodes;
    private final ConsistentHashRing<RedisNode> ring;
    private final AtomicBoolean forcedAvailable = new AtomicBoolean(true);

    public RedisCacheClient(ProxyProperties properties, RuntimeSettings settings) {
        this.settings = settings;
        this.mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.nodes = new ArrayList<>();
        for (String address : properties.getRedisNodes()) {
            nodes.add(new RedisNode(address, properties.getCommandTimeout()));
        }
        this.ring = new ConsistentHashRing<>(nodes, properties.getHashing().getVirtualNodesPerNode(), RedisNode::address);
        log.info("Redis ring ready with {} node(s)", nodes.size());
    }

    public void setForcedAvailable(boolean available) {
        forcedAvailable.set(available);
        log.warn("Redis forced availability set to {}", available);
    }

    public boolean isForcedAvailable() {
        return forcedAvailable.get();
    }

    @Override
    public boolean available() {
        return forcedAvailable.get() && upCount() > 0;
    }

    public int upCount() {
        int up = 0;
        for (RedisNode node : nodes) {
            if (node.isUp()) {
                up++;
            }
        }
        return up;
    }

    public int nodeCount() {
        return nodes.size();
    }

    @Override
    public CacheLookup lookup(String key) {
        if (!forcedAvailable.get() || nodes.isEmpty()) {
            return CacheLookup.unavailable();
        }
        RedisNode node = ring.owner(key);
        if (!node.isUp()) {
            return CacheLookup.unavailable();
        }
        try {
            String raw = node.evalString(LOOKUP_SCRIPT, key);
            if (raw == null) {
                return CacheLookup.miss();
            }
            StoredValue stored = mapper.readValue(raw, StoredValue.class);
            byte[] body = stored.bodyBase64() == null
                    ? new byte[0]
                    : Base64.getDecoder().decode(stored.bodyBase64());
            CacheEntry entry = new CacheEntry(
                    stored.status(),
                    stored.contentType(),
                    body,
                    stored.freshUntil(),
                    stored.staleUntil(),
                    stored.negative()
            );
            long now = System.currentTimeMillis();
            if (entry.fresh(now)) {
                return CacheLookup.fresh(entry);
            }
            if (entry.stale(now)) {
                return CacheLookup.stale(entry);
            }
            return CacheLookup.miss();
        } catch (RuntimeException exception) {
            node.markDown(exception);
            return CacheLookup.unavailable();
        } catch (Exception exception) {
            node.markDown(exception);
            return CacheLookup.unavailable();
        }
    }

    @Override
    public CacheEntry store(String key, int status, String contentType, byte[] body, CachePolicy policy, boolean hot) {
        return null;
    }

    @Override
    public Optional<String> pinGeneration(String key) {
        if (!forcedAvailable.get() || nodes.isEmpty()) {
            return Optional.empty();
        }
        RedisNode node = ring.owner(key);
        if (!node.isUp()) {
            return Optional.empty();
        }
        try {
            String pinned = node.evalString(PIN_SCRIPT, key, newToken(), Long.toString(GENERATION_RETENTION_MS));
            if (pinned == null || pinned.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(pinned);
        } catch (RuntimeException exception) {
            node.markDown(exception);
            return Optional.empty();
        }
    }

    @Override
    public CacheEntry storeIfGeneration(String key, String generation, int status, String contentType, byte[] body, CachePolicy policy, boolean hot) {
        if (!forcedAvailable.get() || nodes.isEmpty() || body.length > MAX_BODY_BYTES || generation == null || generation.isBlank()) {
            return null;
        }
        RedisNode node = ring.owner(key);
        if (!node.isUp()) {
            return null;
        }
        try {
            long now = System.currentTimeMillis();
            boolean negative = status == 404;
            Duration baseTtl = negative
                    ? settings.negativeTtl()
                    : (hot ? policy.hotTtl() : policy.ttl());
            long freshMs = jitter(baseTtl.toMillis(), settings.ttlJitterFraction());
            long freshUntil = now + freshMs;
            long staleUntil = negative ? freshUntil : freshUntil + policy.staleWindow().toMillis();
            long redisTtl = Math.max(1, staleUntil - now);
            CacheEntry entry = new CacheEntry(status, contentType, body, freshUntil, staleUntil, negative);
            StoredValue stored = new StoredValue(
                    status,
                    contentType,
                    Base64.getEncoder().encodeToString(body),
                    freshUntil,
                    staleUntil,
                    negative
            );
            Long written = node.evalLong(
                    STORE_SCRIPT,
                    key,
                    generation,
                    mapper.writeValueAsString(stored),
                    Long.toString(redisTtl)
            );
            if (written == null || written != 1L) {
                return null;
            }
            return entry;
        } catch (RuntimeException exception) {
            node.markDown(exception);
            return null;
        } catch (Exception exception) {
            node.markDown(exception);
            return null;
        }
    }

    @Override
    public void invalidate(String key) {
        if (!forcedAvailable.get() || nodes.isEmpty()) {
            return;
        }
        RedisNode node = ring.owner(key);
        if (!node.isUp()) {
            return;
        }
        try {
            node.evalLong(INVALIDATE_SCRIPT, key, newToken(), Long.toString(GENERATION_RETENTION_MS));
        } catch (RuntimeException exception) {
            node.markDown(exception);
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void probe() {
        for (RedisNode node : nodes) {
            boolean reachable = node.ping();
            if (reachable) {
                node.markUp();
            } else {
                node.markDown(null);
            }
        }
    }

    @PreDestroy
    public void close() {
        for (RedisNode node : nodes) {
            node.close();
        }
    }

    static String newToken() {
        byte[] bytes = new byte[16];
        TOKENS.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    static long jitter(long baseMs, double fraction) {
        if (baseMs <= 1 || fraction <= 0) {
            return Math.max(1, baseMs);
        }
        double delta = (ThreadLocalRandom.current().nextDouble() * 2 - 1) * fraction;
        return Math.max(1, Math.round(baseMs * (1 + delta)));
    }

    static final class RedisNode {
        private static final Logger log = LoggerFactory.getLogger(RedisNode.class);

        private final String address;
        private final Duration timeout;
        private final AtomicBoolean up = new AtomicBoolean(false);
        private volatile RedisClient client;
        private volatile StatefulRedisConnection<String, String> connection;

        RedisNode(String address, Duration timeout) {
            this.address = address;
            this.timeout = timeout;
            try {
                connect();
                up.set(true);
            } catch (RuntimeException exception) {
                up.set(false);
                log.warn("Redis node {} is not reachable at startup: {}", address, exception.getMessage());
            }
        }

        String address() {
            return address;
        }

        boolean isUp() {
            return up.get() && connection != null;
        }

        void markUp() {
            if (up.compareAndSet(false, true)) {
                log.info("Redis node {} recovered", address);
            }
        }

        void markDown(Exception exception) {
            if (up.compareAndSet(true, false)) {
                if (exception == null) {
                    log.warn("Redis node {} marked down", address);
                } else {
                    log.warn("Redis node {} marked down: {}", address, exception.getMessage());
                }
            } else {
                up.set(false);
            }
        }

        String evalString(String script, String key, String... args) {
            return connection().sync().eval(script, ScriptOutputType.VALUE, new String[]{key}, args);
        }

        Long evalLong(String script, String key, String... args) {
            return connection().sync().eval(script, ScriptOutputType.INTEGER, new String[]{key}, args);
        }

        synchronized boolean ping() {
            try {
                if (connection == null) {
                    connect();
                }
                String pong = connection.sync().ping();
                return "PONG".equalsIgnoreCase(pong);
            } catch (RuntimeException exception) {
                closeConnection();
                return false;
            }
        }

        synchronized void close() {
            closeConnection();
            if (client != null) {
                client.shutdown();
                client = null;
            }
        }

        private StatefulRedisConnection<String, String> connection() {
            StatefulRedisConnection<String, String> current = connection;
            if (current == null || !up.get()) {
                throw new IllegalStateException("redis node " + address + " is down");
            }
            return current;
        }

        private synchronized void connect() {
            closeConnection();
            if (client != null) {
                client.shutdown();
                client = null;
            }
            String[] parts = address.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 6379;
            RedisURI uri = RedisURI.builder()
                    .withHost(host)
                    .withPort(port)
                    .withTimeout(timeout)
                    .build();
            client = RedisClient.create(uri);
            client.setOptions(ClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.enabled(timeout))
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .build());
            connection = client.connect();
        }

        private void closeConnection() {
            StatefulRedisConnection<String, String> current = connection;
            connection = null;
            if (current != null) {
                try {
                    current.close();
                } catch (RuntimeException ignored) {
                    // Closing a broken connection is best-effort.
                }
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof RedisNode redisNode)) {
                return false;
            }
            return address.equals(redisNode.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address);
        }
    }
}
