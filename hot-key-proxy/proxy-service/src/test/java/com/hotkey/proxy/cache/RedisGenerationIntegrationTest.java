package com.hotkey.proxy.cache;

import com.hotkey.proxy.config.ProxyProperties;
import com.hotkey.proxy.config.RuntimeSettings;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs the production Redis scripts against one disposable Redis started with the Docker CLI.
 * The container name starts with hk-gen-it- and the host port is not 6379, 6380, or 6381.
 */
class RedisGenerationIntegrationTest {

    private static final CachePolicy POLICY = new CachePolicy(Duration.ofSeconds(60), Duration.ofSeconds(30), Duration.ofSeconds(300));
    private static final int[] STACK_PORTS = {6379, 6380, 6381};

    private static String containerName;
    private static int port;

    private final List<RedisCacheClient> caches = new ArrayList<>();
    private final List<RedisClient> rawClients = new ArrayList<>();

    @BeforeAll
    static void startDisposableRedis() throws Exception {
        port = freePort();
        containerName = "hk-gen-it-" + UUID.randomUUID().toString().substring(0, 12);
        assertTrue(containerName.startsWith("hk-gen-it-"));
        assertFalse(containerName.contains("hot-key-proxy"));
        docker(
                "run", "--name", containerName, "-d",
                "-p", "127.0.0.1:" + port + ":6379",
                "redis:7",
                "redis-server", "--save", "", "--appendonly", "no",
                "--maxmemory", "32mb", "--maxmemory-policy", "allkeys-lru"
        );
        awaitPing();
    }

    @AfterAll
    static void removeDisposableRedis() throws Exception {
        if (containerName == null) {
            return;
        }
        Process process = new ProcessBuilder("docker", "rm", "-f", containerName)
                .redirectErrorStream(true)
                .start();
        process.waitFor();
    }

    @AfterEach
    void closeClients() {
        for (RedisCacheClient cache : caches) {
            cache.close();
        }
        caches.clear();
        for (RedisClient client : rawClients) {
            client.shutdown();
        }
        rawClients.clear();
    }

    @Test
    void pinAndConditionalStoreRoundTrip() {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();

        assertToken(pinned);
        assertEquals(pinned, raw().sync().hget(key, "gen"));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());

        byte[] body = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
        assertNotNull(cache.storeIfGeneration(key, pinned, 200, "application/json", body, POLICY, false));
        CacheLookup lookup = cache.lookup(key);
        assertEquals(CacheLookup.Status.FRESH, lookup.status());
        assertTrue(java.util.Arrays.equals(body, lookup.entry().body()));
    }

    @Test
    void conditionalStoreRejectsADifferentOrAbsentToken() {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();

        assertNull(cache.storeIfGeneration(key, pinned + "aa", 200, "application/json", new byte[]{1}, POLICY, false));
        assertNull(cache.storeIfGeneration(key() + ":absent", pinned, 200, "application/json", new byte[]{1}, POLICY, false));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
        assertNull(raw().sync().hget(key, "body"));
    }

    @Test
    void invalidateRemovesTheBodyAndRejectsThePinnedToken() {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        assertNotNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{1}, POLICY, false));

        cache.invalidate(key);
        String afterFirst = raw().sync().hget(key, "gen");
        cache.invalidate(key);
        String afterSecond = raw().sync().hget(key, "gen");

        assertToken(afterFirst);
        assertToken(afterSecond);
        assertNotEquals(pinned, afterFirst);
        assertNotEquals(afterFirst, afterSecond);
        assertNull(raw().sync().hget(key, "body"));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{2}, POLICY, false));
        assertNull(cache.storeIfGeneration(key, afterFirst, 200, "application/json", new byte[]{3}, POLICY, false));
        assertNotNull(cache.storeIfGeneration(key, afterSecond, 200, "application/json", new byte[]{4}, POLICY, false));
    }

    @Test
    void concurrentInvalidatesReplaceTheToken() throws Exception {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        int writes = 40;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < writes; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    cache.invalidate(key);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        String current = raw().sync().hget(key, "gen");
        assertToken(current);
        assertNotEquals(pinned, current);
        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{1}, POLICY, false));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
    }

    @Test
    void deletingTheKeyWithoutRecreatingItRejectsTheInFlightToken() {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        raw().sync().del(key);

        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{1}, POLICY, false));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
        assertEquals(0L, raw().sync().exists(key));
    }

    @Test
    void deletionThenRecreationDoesNotMatchTheOldToken() {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        raw().sync().del(key);

        String recreated = cache.pinGeneration(key).orElseThrow();

        assertNotEquals(pinned, recreated);
        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{9}, POLICY, false));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
    }

    @Test
    void expiryThenRecreationDoesNotMatchTheOldToken() throws Exception {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        raw().sync().pexpire(key, 200);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (raw().sync().exists(key) > 0) {
            if (System.nanoTime() > deadline) {
                fail("disposable key did not expire");
            }
            Thread.sleep(50);
        }

        String recreated = cache.pinGeneration(key).orElseThrow();

        assertNotEquals(pinned, recreated);
        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{9}, POLICY, false));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
    }

    @Test
    void fenceAfterDeletionDoesNotMatchTheOldToken() {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        raw().sync().del(key);

        cache.invalidate(key);
        String fenced = raw().sync().hget(key, "gen");

        assertToken(fenced);
        assertNotEquals(pinned, fenced);
        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{9}, POLICY, false));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
    }

    @Test
    void restartThenRecreationDoesNotMatchTheOldToken() throws Exception {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        raw().sync().configSet("save", "");
        raw().sync().configSet("appendonly", "no");

        docker("restart", "-t", "5", containerName);
        awaitUp(cache);
        assertEquals(0L, raw().sync().exists(key));

        String recreated = cache.pinGeneration(key).orElseThrow();

        assertNotEquals(pinned, recreated);
        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{9}, POLICY, false));
        assertEquals(CacheLookup.Status.MISS, cache.lookup(key).status());
    }

    @Test
    void deadRedisFailsClosed() {
        ProxyProperties properties = new ProxyProperties();
        properties.setRedisNodes(List.of("127.0.0.1:1"));
        properties.setCommandTimeout(Duration.ofMillis(300));
        RedisCacheClient cache = cache(properties);

        assertFalse(cache.available());
        assertEquals(Optional.empty(), cache.pinGeneration(key()));
        assertNull(cache.storeIfGeneration(key(), "dead-token", 200, "application/json", new byte[]{1}, POLICY, false));
        cache.invalidate(key());
        assertEquals(CacheLookup.Status.UNAVAILABLE, cache.lookup(key()).status());
    }

    @Test
    void forcedUnavailabilityDoesNotWriteTheLiveKey() {
        RedisCacheClient cache = cache();
        String key = key();
        String pinned = cache.pinGeneration(key).orElseThrow();
        cache.setForcedAvailable(false);

        assertEquals(Optional.empty(), cache.pinGeneration(key));
        assertNull(cache.storeIfGeneration(key, pinned, 200, "application/json", new byte[]{1}, POLICY, false));
        cache.invalidate(key);
        assertEquals(pinned, raw().sync().hget(key, "gen"));
        assertNull(raw().sync().hget(key, "body"));
    }

    private static void assertToken(String token) {
        assertNotNull(token);
        assertTrue(token.matches("[0-9a-f]{32}"), token);
    }

    private static void awaitPing() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            RedisClient client = RedisClient.create(RedisURI.builder().withHost("127.0.0.1").withPort(port).withTimeout(Duration.ofMillis(500)).build());
            try (StatefulRedisConnection<String, String> connection = client.connect()) {
                if ("PONG".equalsIgnoreCase(connection.sync().ping())) {
                    client.shutdown();
                    return;
                }
            } catch (RuntimeException exception) {
                last = exception;
            } finally {
                client.shutdown();
            }
            Thread.sleep(200);
        }
        fail("disposable redis did not answer ping: " + last);
    }

    private static void awaitUp(RedisCacheClient cache) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            cache.probe();
            if (cache.upCount() > 0 && rawPing()) {
                return;
            }
            Thread.sleep(200);
        }
        fail("disposable redis did not recover");
    }

    private static boolean rawPing() {
        RedisClient client = RedisClient.create(RedisURI.builder()
                .withHost("127.0.0.1")
                .withPort(port)
                .withTimeout(Duration.ofMillis(500))
                .build());
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            return "PONG".equalsIgnoreCase(connection.sync().ping());
        } catch (RuntimeException exception) {
            return false;
        } finally {
            client.shutdown();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            int candidate = socket.getLocalPort();
            for (int stackPort : STACK_PORTS) {
                if (candidate == stackPort) {
                    fail("refusing to bind stack Redis port " + candidate);
                }
            }
            return candidate;
        }
    }

    private static void docker(String... args) throws Exception {
        String[] command = new String[args.length + 1];
        command[0] = "docker";
        System.arraycopy(args, 0, command, 1, args.length);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int code = process.waitFor();
        if (code != 0) {
            fail(String.join(" ", command) + " exited " + code + ": " + output);
        }
    }

    private RedisCacheClient cache() {
        ProxyProperties properties = new ProxyProperties();
        properties.setRedisNodes(List.of("127.0.0.1:" + port));
        properties.setCommandTimeout(Duration.ofSeconds(2));
        return cache(properties);
    }

    private RedisCacheClient cache(ProxyProperties properties) {
        RedisCacheClient cache = new RedisCacheClient(properties, new RuntimeSettings(properties));
        caches.add(cache);
        return cache;
    }

    private StatefulRedisConnection<String, String> raw() {
        RedisClient client = RedisClient.create(RedisURI.builder().withHost("127.0.0.1").withPort(port).build());
        rawClients.add(client);
        return client.connect();
    }

    private static String key() {
        return "it:gen:" + UUID.randomUUID();
    }
}
