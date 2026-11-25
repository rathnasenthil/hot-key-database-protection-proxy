package com.hotkey.proxy;

import com.hotkey.proxy.cache.CacheEntry;
import com.hotkey.proxy.cache.LocalHotCache;
import com.hotkey.proxy.coalesce.CoalescingEngine;
import com.hotkey.proxy.config.ProxyProperties;
import com.hotkey.proxy.config.RuntimeSettings;
import com.hotkey.proxy.core.ReadCoordinator;
import com.hotkey.proxy.core.WriteCoordinator;
import com.hotkey.proxy.hotkey.HotKeyDetector;
import com.hotkey.proxy.observe.ProxyMetrics;
import com.hotkey.proxy.origin.OriginGateway;
import com.hotkey.proxy.origin.OriginResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A passing test here documents a hole that this mitigation does not close.
 * Do not cite these results as safety guarantees.
 */
class WritePathKnownLimitationsTest {

    @Test
    void knownLimitation_evictedGenerationLetsANewReadCacheThePreCommitSnapshot() {
        StatefulCache cache = new StatefulCache();
        byte[] preCommit = "{\"id\":12,\"price\":1.00}".getBytes(StandardCharsets.UTF_8);
        String pinned = cache.pinGeneration("product:12").orElseThrow();
        cache.evict("product:12");
        assertNull(cache.storeIfGeneration("product:12", pinned, 200, "application/json", preCommit, null, false));

        AtomicInteger calls = new AtomicInteger();
        ReadCoordinator reads = reader(cache, new LocalHotCache(), (method, path, body, contentType) -> {
            calls.incrementAndGet();
            return OriginResult.ok(200, "application/json", preCommit);
        });

        ResponseEntity<byte[]> filled = reads.read("product", "product:12", "/api/products/12");
        ResponseEntity<byte[]> hit = reads.read("product", "product:12", "/api/products/12");

        assertEquals("MISS", filled.getHeaders().getFirst("X-Cache"));
        assertEquals("HIT", hit.getHeaders().getFirst("X-Cache"));
        assertTrue(java.util.Arrays.equals(preCommit, hit.getBody()));
        assertEquals(1, calls.get());
    }

    @Test
    void knownLimitation_timeoutFenceDoesNotBoundALateCommitByThePreviousTtl() {
        StatefulCache cache = new StatefulCache();
        LocalHotCache local = new LocalHotCache();
        byte[] beforeCommit = "{\"id\":11,\"price\":1.00}".getBytes(StandardCharsets.UTF_8);
        byte[] committed = "{\"id\":11,\"price\":9.00}".getBytes(StandardCharsets.UTF_8);
        AtomicReference<byte[]> originBody = new AtomicReference<>(beforeCommit);
        AtomicInteger reads = new AtomicInteger();
        ReadCoordinator coordinator = reader(cache, local, (method, path, body, contentType) -> {
            reads.incrementAndGet();
            return OriginResult.ok(200, "application/json", originBody.get());
        });
        WriteCoordinator writes = writer(cache, local, (method, path, body, contentType) -> OriginResult.unknown("timeout"));

        long beforeStore = System.currentTimeMillis();
        ResponseEntity<byte[]> write = writes.forward(
                HttpMethod.PUT, "/api/products/11", committed, "application/json", List.of("product:11"));
        ResponseEntity<byte[]> cached = coordinator.read("product", "product:11", "/api/products/11");
        originBody.set(committed);
        ResponseEntity<byte[]> follow = coordinator.read("product", "product:11", "/api/products/11");

        assertEquals(504, write.getStatusCode().value());
        assertEquals("MISS", cached.getHeaders().getFirst("X-Cache"));
        assertEquals("HIT", follow.getHeaders().getFirst("X-Cache"));
        assertTrue(java.util.Arrays.equals(beforeCommit, follow.getBody()));
        assertEquals(1, reads.get());
        Long freshUntil = cache.freshUntil("product:11");
        assertTrue(freshUntil != null && freshUntil - beforeStore >= 59_000);
    }

    @Test
    void knownLimitation_otherProxyLocalCacheStillServesThePreWriteValue() {
        StatefulCache shared = new StatefulCache();
        LocalHotCache handledHere = new LocalHotCache();
        LocalHotCache otherProxy = new LocalHotCache();
        byte[] oldBody = "{\"id\":8,\"price\":1.00}".getBytes(StandardCharsets.UTF_8);
        byte[] newBody = "{\"id\":8,\"price\":2.00}".getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        otherProxy.put("product:8", new CacheEntry(200, "application/json", oldBody, now + 60_000, now + 90_000, false));
        AtomicInteger otherOriginCalls = new AtomicInteger();
        ReadCoordinator otherReads = reader(shared, otherProxy, (method, path, body, contentType) -> {
            otherOriginCalls.incrementAndGet();
            return OriginResult.ok(200, "application/json", newBody);
        });
        WriteCoordinator writes = writer(shared, handledHere, (method, path, body, contentType) ->
                OriginResult.ok(200, "application/json", newBody));

        ResponseEntity<byte[]> write = writes.forward(
                HttpMethod.PUT, "/api/products/8", newBody, "application/json", List.of("product:8"));
        ResponseEntity<byte[]> otherHit = otherReads.read("product", "product:8", "/api/products/8");

        assertEquals(200, write.getStatusCode().value());
        assertNull(handledHere.getIfFresh("product:8"));
        assertEquals("HIT", otherHit.getHeaders().getFirst("X-Cache"));
        assertTrue(java.util.Arrays.equals(oldBody, otherHit.getBody()));
        assertEquals(0, otherOriginCalls.get());
    }

    @Test
    void knownLimitation_eightSecondsDoesNotResolveAMultiStatementTransaction() {
        long poolWaitMs = 5_000;
        long statementMs = 2_000;
        int statements = 3;
        long thirdStatementStillRunningAt = poolWaitMs + statementMs * (statements - 1);
        assertTrue(thirdStatementStillRunningAt > 8_000);
    }

    private static ReadCoordinator reader(StatefulCache cache, LocalHotCache local, OriginGateway origin) {
        ProxyProperties properties = new ProxyProperties();
        properties.getCoalescing().setMaxWait(Duration.ofSeconds(2));
        return new ReadCoordinator(
                cache,
                local,
                new CoalescingEngine(),
                new HotKeyDetector(Duration.ofSeconds(5), () -> 100_000, () -> 0.5, System::currentTimeMillis),
                origin,
                new RuntimeSettings(properties),
                new ProxyMetrics(new SimpleMeterRegistry()),
                properties
        );
    }

    private static WriteCoordinator writer(StatefulCache cache, LocalHotCache local, OriginGateway origin) {
        ProxyProperties properties = new ProxyProperties();
        return new WriteCoordinator(origin, cache, local, new RuntimeSettings(properties), properties);
    }
}
