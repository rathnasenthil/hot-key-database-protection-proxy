package com.hotkey.proxy.core;

import com.hotkey.proxy.cache.CacheEntry;
import com.hotkey.proxy.cache.CacheLookup;
import com.hotkey.proxy.cache.CachePolicy;
import com.hotkey.proxy.cache.LocalHotCache;
import com.hotkey.proxy.cache.ResponseCache;
import com.hotkey.proxy.coalesce.CoalesceResult;
import com.hotkey.proxy.coalesce.CoalescingEngine;
import com.hotkey.proxy.config.ProxyProperties;
import com.hotkey.proxy.config.RuntimeSettings;
import com.hotkey.proxy.hotkey.HotKeyDetector;
import com.hotkey.proxy.observe.ProxyMetrics;
import com.hotkey.proxy.origin.OriginGateway;
import com.hotkey.proxy.origin.OriginResult;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ReadCoordinator {

    private final ResponseCache cache;
    private final LocalHotCache localCache;
    private final CoalescingEngine coalescer;
    private final HotKeyDetector hotKeys;
    private final OriginGateway origin;
    private final RuntimeSettings settings;
    private final ProxyMetrics metrics;
    private final String instanceId;
    private final ConcurrentHashMap<String, Boolean> refreshing = new ConcurrentHashMap<>();

    public ReadCoordinator(
            ResponseCache cache,
            LocalHotCache localCache,
            CoalescingEngine coalescer,
            HotKeyDetector hotKeys,
            OriginGateway origin,
            RuntimeSettings settings,
            ProxyMetrics metrics,
            ProxyProperties properties
    ) {
        this.cache = cache;
        this.localCache = localCache;
        this.coalescer = coalescer;
        this.hotKeys = hotKeys;
        this.origin = origin;
        this.settings = settings;
        this.metrics = metrics;
        this.instanceId = properties.getInstanceId();
    }

    public ResponseEntity<byte[]> read(String keyType, String cacheKey, String originPath) {
        long start = System.nanoTime();
        boolean hot = hotKeys.record(cacheKey);
        CachePolicy policy = settings.policy(keyType);

        if (cache.available()) {
            CacheEntry local = localCache.getIfFresh(cacheKey);
            if (local != null) {
                return respond(local.status(), local.contentType(), local.body(), "HIT", true, start);
            }
        }

        CacheLookup lookup = cache.lookup(cacheKey);
        if (lookup.status() == CacheLookup.Status.FRESH) {
            rememberHot(cacheKey, lookup.entry(), hot);
            return respond(lookup.entry().status(), lookup.entry().contentType(), lookup.entry().body(), "HIT", hot, start);
        }
        if (lookup.status() == CacheLookup.Status.STALE) {
            refreshAsync(cacheKey, originPath, policy, hot);
            return respond(lookup.entry().status(), lookup.entry().contentType(), lookup.entry().body(), "STALE", hot, start);
        }

        boolean bypass = lookup.status() == CacheLookup.Status.UNAVAILABLE;
        try {
            CoalesceResult<Payload> result = coalescer.execute(
                    cacheKey,
                    settings.coalesceMaxWait(),
                    () -> load(cacheKey, originPath, policy, hot, bypass),
                    () -> fallback(cacheKey, originPath, policy, hot, bypass)
            );
            Payload payload = result.value();
            String header = headerFor(result, bypass);
            if (result.shared()) {
                metrics.coalesced();
            }
            return respond(payload.status(), payload.contentType(), payload.body(), header, hot, start);
        } catch (OriginRejectedException exception) {
            return rejected(hot, bypass, start);
        } catch (OriginFailedException exception) {
            return failed(hot, bypass, start);
        }
    }

    private Payload load(String cacheKey, String originPath, CachePolicy policy, boolean hot, boolean bypass) {
        if (origin.preferStale()) {
            Payload stale = reusable(cacheKey);
            if (stale != null) {
                return stale;
            }
        }
        // Pin before the origin call. A later store matches only this token.
        // An empty pin means generation state is unavailable and the store is refused.
        Optional<String> generation = bypass ? Optional.empty() : cache.pinGeneration(cacheKey);
        OriginResult result = origin.execute(HttpMethod.GET, originPath, null, null);
        if (result.kind() == OriginResult.Kind.REJECTED) {
            Payload stale = reusable(cacheKey);
            if (stale != null) {
                return stale;
            }
            throw new OriginRejectedException();
        }
        if (result.kind() == OriginResult.Kind.FAILED
                || result.kind() == OriginResult.Kind.UNKNOWN
                || result.status() >= 500) {
            Payload stale = reusable(cacheKey);
            if (stale != null) {
                return stale;
            }
            throw new OriginFailedException();
        }
        String contentType = result.contentType() == null ? "application/json" : result.contentType();
        byte[] body = result.body() == null ? new byte[0] : result.body();
        if (!bypass && generation.isPresent()) {
            CacheEntry stored = cache.storeIfGeneration(
                    cacheKey,
                    generation.get(),
                    result.status(),
                    contentType,
                    body,
                    policy,
                    hot
            );
            if (stored != null) {
                rememberHot(cacheKey, stored, hot);
            }
        }
        return new Payload(result.status(), contentType, body, null);
    }

    private Payload fallback(String cacheKey, String originPath, CachePolicy policy, boolean hot, boolean bypass) {
        Payload cached = reusable(cacheKey);
        if (cached != null) {
            return cached;
        }
        return load(cacheKey, originPath, policy, hot, bypass);
    }

    private Payload reusable(String cacheKey) {
        CacheLookup lookup = cache.lookup(cacheKey);
        if (lookup.entry() == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        CacheEntry entry = lookup.entry();
        if (entry.fresh(now) || entry.stale(now)) {
            String header = entry.fresh(now) ? "HIT" : "STALE";
            return new Payload(entry.status(), entry.contentType(), entry.body(), header);
        }
        return null;
    }

    private void refreshAsync(String cacheKey, String originPath, CachePolicy policy, boolean hot) {
        if (refreshing.putIfAbsent(cacheKey, Boolean.TRUE) != null) {
            return;
        }
        Thread.startVirtualThread(() -> {
            try {
                coalescer.execute(
                        cacheKey,
                        java.time.Duration.ofSeconds(2),
                        () -> load(cacheKey, originPath, policy, hot, false),
                        () -> new Payload(0, "application/json", new byte[0], null)
                );
            } catch (RuntimeException ignored) {
                // The caller already received the stale value. A failed refresh keeps it.
            } finally {
                refreshing.remove(cacheKey);
            }
        });
    }

    private void rememberHot(String cacheKey, CacheEntry entry, boolean hot) {
        if (hot && entry != null) {
            localCache.put(cacheKey, entry);
        }
    }

    private static String headerFor(CoalesceResult<Payload> result, boolean bypass) {
        if (bypass) {
            return "BYPASS";
        }
        if (result.value().headerOverride() != null) {
            return result.value().headerOverride();
        }
        if (result.shared()) {
            return "COALESCED";
        }
        return "MISS";
    }

    private ResponseEntity<byte[]> rejected(boolean hot, boolean bypass, long start) {
        byte[] body = "{\"error\":\"origin overloaded\"}".getBytes(StandardCharsets.UTF_8);
        return respond(429, "application/json", body, bypass ? "BYPASS" : "MISS", hot, start);
    }

    private ResponseEntity<byte[]> failed(boolean hot, boolean bypass, long start) {
        byte[] body = "{\"error\":\"origin unavailable\"}".getBytes(StandardCharsets.UTF_8);
        return respond(502, "application/json", body, bypass ? "BYPASS" : "MISS", hot, start);
    }

    private ResponseEntity<byte[]> respond(int status, String contentType, byte[] body, String cacheHeader, boolean hot, long start) {
        metrics.cacheResult(cacheHeader, System.nanoTime() - start);
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.CONTENT_TYPE, contentType == null ? "application/json" : contentType);
        headers.set("X-Cache", cacheHeader);
        headers.set("X-Hot-Key", Boolean.toString(hot));
        headers.set("X-Proxy-Instance", instanceId);
        if (status == 429) {
            headers.set(HttpHeaders.RETRY_AFTER, Integer.toString(settings.retryAfterSeconds()));
        }
        return new ResponseEntity<>(body == null ? new byte[0] : body, headers, status);
    }

    private record Payload(int status, String contentType, byte[] body, String headerOverride) {
    }
}
