package com.hotkey.proxy;

import com.hotkey.proxy.admission.OriginBulkhead;
import com.hotkey.proxy.cache.CacheEntry;
import com.hotkey.proxy.cache.CacheLookup;
import com.hotkey.proxy.cache.CachePolicy;
import com.hotkey.proxy.cache.LocalHotCache;
import com.hotkey.proxy.cache.ResponseCache;
import com.hotkey.proxy.coalesce.CoalesceResult;
import com.hotkey.proxy.coalesce.CoalescingEngine;
import com.hotkey.proxy.config.ProxyProperties;
import com.hotkey.proxy.config.RuntimeSettings;
import com.hotkey.proxy.core.ReadCoordinator;
import com.hotkey.proxy.core.WriteCoordinator;
import org.springframework.web.client.ResourceAccessException;
import com.hotkey.proxy.hash.ConsistentHashRing;
import com.hotkey.proxy.hotkey.HotKeyDetector;
import com.hotkey.proxy.observe.ProxyMetrics;
import com.hotkey.proxy.origin.HttpOriginGateway;
import com.hotkey.proxy.origin.OriginGateway;
import com.hotkey.proxy.origin.OriginResult;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ProtectionCoreTest {

    @Test
    void ringKeepsAKeyOnTheSameNodeAndSpreadsTheSpace() {
        List<String> nodes = List.of("redis-1", "redis-2", "redis-3");
        ConsistentHashRing<String> ring = new ConsistentHashRing<>(nodes, 150, node -> node);
        assertEquals(ring.owner("product:42"), ring.owner("product:42"));
        assertEquals(ring.owner("product:42"), ring.owners("product:42", 3).get(0));
        assertEquals(3, ring.owners("product:42", 3).stream().distinct().count());

        Map<String, Integer> counts = new ConcurrentHashMap<>();
        for (int i = 0; i < 20_000; i++) {
            counts.merge(ring.owner("product:" + i), 1, Integer::sum);
        }
        for (String node : nodes) {
            int count = counts.getOrDefault(node, 0);
            assertTrue(count > 4_000 && count < 10_000, node + " received " + count);
        }
    }

    @Test
    void coalescerRunsTheLeaderOnce() throws Exception {
        CoalescingEngine engine = new CoalescingEngine();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Supplier<String> leader = () -> {
            calls.incrementAndGet();
            started.countDown();
            try {
                assertTrue(release.await(2, TimeUnit.SECONDS));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            return "value";
        };

        ExecutorService pool = Executors.newFixedThreadPool(10);
        try {
            Future<CoalesceResult<String>> first = pool.submit(() ->
                    engine.execute("product:1", Duration.ofSeconds(2), leader, () -> "fallback"));
            assertTrue(started.await(1, TimeUnit.SECONDS));
            List<Future<CoalesceResult<String>>> waiters = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                waiters.add(pool.submit(() ->
                        engine.execute("product:1", Duration.ofSeconds(2), leader, () -> "fallback")));
            }
            Thread.sleep(200);
            release.countDown();
            CoalesceResult<String> lead = first.get(2, TimeUnit.SECONDS);
            assertTrue(lead.leader());
            assertEquals("value", lead.value());
            int shared = 0;
            for (Future<CoalesceResult<String>> waiter : waiters) {
                CoalesceResult<String> result = waiter.get(2, TimeUnit.SECONDS);
                assertEquals("value", result.value());
                if (result.shared()) {
                    shared++;
                }
            }
            assertEquals(1, calls.get());
            assertTrue(shared >= 1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void hotKeyPromotesAndDemotesWithHysteresis() {
        AtomicLong clock = new AtomicLong(1_000_000);
        HotKeyDetector detector = new HotKeyDetector(Duration.ofSeconds(5), () -> 10, () -> 0.5, clock::get);
        for (int i = 0; i < 49; i++) {
            detector.record("product:1");
        }
        assertFalse(detector.isHot("product:1"));
        detector.record("product:1");
        assertTrue(detector.isHot("product:1"));
        clock.addAndGet(5_000);
        detector.record("product:1");
        assertFalse(detector.isHot("product:1"));
    }

    @Test
    void bulkheadRejectsWorkBeyondTheQueue() {
        OriginBulkhead gate = new OriginBulkhead(1, 0, Duration.ofMillis(20), 10, 0.5);
        assertTrue(gate.tryEnter());
        assertFalse(gate.tryEnter());
        gate.leave();
        assertTrue(gate.tryEnter());
        gate.leave();
        assertEquals(0, gate.inUse());
    }

    @Test
    void freshCacheHitSkipsOrigin() {
        FakeCache cache = new FakeCache();
        cache.lookup = CacheLookup.fresh(entry(200, "{\"id\":1}", 60_000));
        AtomicInteger calls = new AtomicInteger();
        ReadCoordinator coordinator = coordinator(cache, (method, path, body, contentType) -> {
            calls.incrementAndGet();
            return OriginResult.failed();
        });

        ResponseEntity<byte[]> response = coordinator.read("product", "product:1", "/api/products/1");

        assertEquals(200, response.getStatusCode().value());
        assertEquals("HIT", response.getHeaders().getFirst("X-Cache"));
        assertEquals(0, calls.get());
    }

    @Test
    void staleHitReturnsWithoutWaitingForOrigin() {
        FakeCache cache = new FakeCache();
        long now = System.currentTimeMillis();
        cache.lookup = CacheLookup.stale(new CacheEntry(200, "application/json", "{\"id\":7}".getBytes(StandardCharsets.UTF_8), now - 1_000, now + 30_000, false));
        CountDownLatch originEntered = new CountDownLatch(1);
        ReadCoordinator coordinator = coordinator(cache, (method, path, body, contentType) -> {
            originEntered.countDown();
            try {
                Thread.sleep(400);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            return OriginResult.ok(200, "application/json", "{\"id\":7,\"fresh\":true}".getBytes(StandardCharsets.UTF_8));
        });

        long started = System.nanoTime();
        ResponseEntity<byte[]> response = coordinator.read("product", "product:7", "/api/products/7");
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;

        assertEquals("STALE", response.getHeaders().getFirst("X-Cache"));
        assertTrue(elapsedMs < 150, "stale response took " + elapsedMs + "ms");
        assertEquals(200, response.getStatusCode().value());
    }

    @Test
    void concurrentMissesShareOneOriginQuery() throws Exception {
        FakeCache cache = new FakeCache();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch joined = new CountDownLatch(6);
        ProxyProperties properties = new ProxyProperties();
        properties.getCoalescing().setMaxWait(Duration.ofSeconds(2));
        ReadCoordinator coordinator = new ReadCoordinator(
                cache,
                new LocalHotCache(),
                new CoalescingEngine() {
                    @Override
                    protected void joinedInFlight(String key) {
                        joined.countDown();
                    }
                },
                new HotKeyDetector(Duration.ofSeconds(5), () -> 100_000, () -> 0.5, System::currentTimeMillis),
                (method, path, body, contentType) -> {
                    if (calls.incrementAndGet() == 1) {
                        started.countDown();
                        try {
                            assertTrue(release.await(2, TimeUnit.SECONDS));
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return OriginResult.ok(200, "application/json", "{\"id\":1}".getBytes(StandardCharsets.UTF_8));
                },
                new RuntimeSettings(properties),
                new ProxyMetrics(new SimpleMeterRegistry()),
                properties
        );

        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            Future<ResponseEntity<byte[]>> leader = pool.submit(() ->
                    coordinator.read("product", "product:1", "/api/products/1"));
            assertTrue(started.await(1, TimeUnit.SECONDS));
            List<Future<ResponseEntity<byte[]>>> waiters = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                waiters.add(pool.submit(() -> coordinator.read("product", "product:1", "/api/products/1")));
            }
            assertTrue(joined.await(1, TimeUnit.SECONDS));
            release.countDown();
            assertEquals("MISS", leader.get(2, TimeUnit.SECONDS).getHeaders().getFirst("X-Cache"));
            int coalesced = 0;
            for (Future<ResponseEntity<byte[]>> waiter : waiters) {
                ResponseEntity<byte[]> response = waiter.get(2, TimeUnit.SECONDS);
                assertEquals(200, response.getStatusCode().value());
                if ("COALESCED".equals(response.getHeaders().getFirst("X-Cache"))) {
                    coalesced++;
                }
            }
            assertEquals(1, calls.get());
            assertTrue(coalesced >= 1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void redisOutageBypassesCacheAndStillServesOrigin() {
        FakeCache cache = new FakeCache();
        cache.available = false;
        cache.lookup = CacheLookup.unavailable();
        ReadCoordinator coordinator = coordinator(cache, (method, path, body, contentType) ->
                OriginResult.ok(200, "application/json", "{\"id\":3}".getBytes(StandardCharsets.UTF_8)));

        ResponseEntity<byte[]> response = coordinator.read("product", "product:3", "/api/products/3");

        assertEquals(200, response.getStatusCode().value());
        assertEquals("BYPASS", response.getHeaders().getFirst("X-Cache"));
        assertEquals(0, cache.stores.get());
    }

    @Test
    void overloadedOriginReturns429() {
        FakeCache cache = new FakeCache();
        ReadCoordinator coordinator = coordinator(cache, (method, path, body, contentType) -> OriginResult.rejected());

        ResponseEntity<byte[]> response = coordinator.read("product", "product:9", "/api/products/9");

        assertEquals(429, response.getStatusCode().value());
        assertEquals("1", response.getHeaders().getFirst("Retry-After"));
    }

    @Test
    void successfulWriteInvalidatesTheAffectedKey() {
        FakeCache cache = new FakeCache();
        WriteCoordinator writes = new WriteCoordinator(
                (method, path, body, contentType) -> OriginResult.ok(200, "application/json", "{}".getBytes(StandardCharsets.UTF_8)),
                cache,
                new LocalHotCache(),
                new RuntimeSettings(new ProxyProperties()),
                new ProxyProperties()
        );

        ResponseEntity<byte[]> response = writes.forward(
                HttpMethod.PUT,
                "/api/products/4",
                "{}".getBytes(StandardCharsets.UTF_8),
                "application/json",
                List.of("product:4")
        );

        assertEquals(200, response.getStatusCode().value());
        assertEquals(List.of("product:4"), cache.invalidated);
    }

    @Test
    void warmCachedReadIsInvalidatedByASuccessfulWrite() {
        StatefulCache cache = new StatefulCache();
        LocalHotCache local = new LocalHotCache();
        byte[] cached = "{\"id\":7,\"price\":1.00}".getBytes(StandardCharsets.UTF_8);
        byte[] updated = "{\"id\":7,\"price\":2.00}".getBytes(StandardCharsets.UTF_8);
        cache.preload("product:7", entry(200, "{\"id\":7,\"price\":1.00}", 60_000));
        AtomicInteger calls = new AtomicInteger();
        ReadCoordinator reads = reader(cache, local, (method, path, body, contentType) -> {
            calls.incrementAndGet();
            return OriginResult.ok(200, "application/json", updated);
        });
        WriteCoordinator writes = writer(cache, local, (method, path, body, contentType) ->
                OriginResult.ok(200, "application/json", updated));

        ResponseEntity<byte[]> hit = reads.read("product", "product:7", "/api/products/7");
        assertEquals("HIT", hit.getHeaders().getFirst("X-Cache"));
        assertTrue(Arrays.equals(cached, hit.getBody()));
        assertEquals(0, calls.get());

        ResponseEntity<byte[]> write = writes.forward(
                HttpMethod.PUT, "/api/products/7", updated, "application/json", List.of("product:7"));
        assertEquals(200, write.getStatusCode().value());
        assertEquals(CacheLookup.Status.MISS, cache.lookup("product:7").status());

        ResponseEntity<byte[]> after = reads.read("product", "product:7", "/api/products/7");
        assertEquals("MISS", after.getHeaders().getFirst("X-Cache"));
        assertEquals(1, calls.get());
        assertTrue(Arrays.equals(updated, after.getBody()));
    }

    @Test
    void coldInFlightReadDoesNotRepopulateThePreWriteValue() throws Exception {
        StatefulCache cache = new StatefulCache();
        LocalHotCache local = new LocalHotCache();
        byte[] oldBody = "{\"id\":8,\"price\":1.00}".getBytes(StandardCharsets.UTF_8);
        byte[] newBody = "{\"id\":8,\"price\":2.00}".getBytes(StandardCharsets.UTF_8);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        ReadCoordinator reads = reader(cache, local, (method, path, body, contentType) -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                entered.countDown();
                try {
                    if (!release.await(2, TimeUnit.SECONDS)) {
                        return OriginResult.failed();
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return OriginResult.failed();
                }
                return OriginResult.ok(200, "application/json", oldBody);
            }
            return OriginResult.ok(200, "application/json", newBody);
        });
        WriteCoordinator writes = writer(cache, local, (method, path, body, contentType) ->
                OriginResult.ok(200, "application/json", newBody));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<ResponseEntity<byte[]>> inFlight = pool.submit(() ->
                    reads.read("product", "product:8", "/api/products/8"));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            ResponseEntity<byte[]> write = writes.forward(
                    HttpMethod.PUT, "/api/products/8", newBody, "application/json", List.of("product:8"));
            assertEquals(200, write.getStatusCode().value());
            assertTrue(cache.events().stream().anyMatch(event -> event.equals("invalidate:product:8")));
            release.countDown();

            ResponseEntity<byte[]> first = inFlight.get(2, TimeUnit.SECONDS);
            assertEquals(200, first.getStatusCode().value());
            assertEquals("MISS", first.getHeaders().getFirst("X-Cache"));
            assertTrue(Arrays.equals(oldBody, first.getBody()));

            ResponseEntity<byte[]> followUp = reads.read("product", "product:8", "/api/products/8");
            boolean staleHit = "HIT".equals(followUp.getHeaders().getFirst("X-Cache"))
                    && Arrays.equals(oldBody, followUp.getBody())
                    && calls.get() == 1;
            if (staleHit) {
                fail("DEFECT: a successful write invalidated the key, then the in-flight cold read stored the pre-write body. The next read was HIT without a new origin call. events=" + cache.events());
            }
            assertEquals("MISS", followUp.getHeaders().getFirst("X-Cache"));
            assertEquals(2, calls.get());
            assertTrue(Arrays.equals(newBody, followUp.getBody()));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void lostPinnedGenerationIsNotTreatedAsAMatch() {
        StatefulCache cache = new StatefulCache();
        cache.dropPinnedGenerationBeforeStore();
        LocalHotCache local = new LocalHotCache();
        byte[] body = "{\"id\":8}".getBytes(StandardCharsets.UTF_8);
        AtomicInteger calls = new AtomicInteger();
        ReadCoordinator reads = reader(cache, local, (method, path, requestBody, contentType) -> {
            calls.incrementAndGet();
            return OriginResult.ok(200, "application/json", body);
        });

        ResponseEntity<byte[]> first = reads.read("product", "product:8", "/api/products/8");

        assertEquals("MISS", first.getHeaders().getFirst("X-Cache"));
        assertEquals(1, calls.get());
        assertEquals(CacheLookup.Status.MISS, cache.lookup("product:8").status());
        assertTrue(cache.events().stream().anyMatch(event -> event.equals("reject-store:product:8")));
        assertTrue(cache.events().stream().noneMatch(event -> event.startsWith("store:")));
    }

    @Test
    void unavailableGenerationFailsClosed() {
        StatefulCache cache = new StatefulCache();
        cache.refusePins();
        AtomicInteger calls = new AtomicInteger();
        ReadCoordinator reads = reader(cache, new LocalHotCache(), (method, path, body, contentType) -> {
            calls.incrementAndGet();
            return OriginResult.ok(200, "application/json", "{\"id\":8}".getBytes(StandardCharsets.UTF_8));
        });

        reads.read("product", "product:8", "/api/products/8");
        reads.read("product", "product:8", "/api/products/8");

        assertEquals(2, calls.get());
        assertTrue(cache.events().stream().noneMatch(event -> event.startsWith("store:")));
    }

    @Test
    void absentGenerationDoesNotMatchAStore() {
        StatefulCache cache = new StatefulCache();
        CacheEntry stored = cache.storeIfGeneration(
                "product:8",
                "absent-token",
                200,
                "application/json",
                "{}".getBytes(StandardCharsets.UTF_8),
                null,
                false
        );

        assertNull(stored);
        assertEquals(CacheLookup.Status.MISS, cache.lookup("product:8").status());
        assertNull(cache.generation("product:8"));
    }

    @Test
    void overlappingWritesOnlyAdvanceGeneration() {
        StatefulCache cache = new StatefulCache();
        WriteCoordinator writes = writer(cache, new LocalHotCache(), (method, path, body, contentType) ->
                OriginResult.ok(200, "application/json", "{}".getBytes(StandardCharsets.UTF_8)));
        String pinned = cache.pinGeneration("product:5").orElseThrow();

        writes.forward(HttpMethod.PUT, "/api/products/5", new byte[0], "application/json", List.of("product:5"));
        String afterFirst = cache.generation("product:5");
        writes.forward(HttpMethod.PUT, "/api/products/5", new byte[0], "application/json", List.of("product:5"));
        String afterSecond = cache.generation("product:5");

        assertNotEquals(pinned, afterFirst);
        assertNotEquals(afterFirst, afterSecond);
        assertNull(cache.storeIfGeneration("product:5", pinned, 200, "application/json", new byte[]{1}, null, false));
        assertNull(cache.storeIfGeneration("product:5", afterFirst, 200, "application/json", new byte[]{1}, null, false));
        assertTrue(cache.storeIfGeneration("product:5", afterSecond, 200, "application/json", new byte[]{1}, null, false) != null);
    }

    @Test
    void timeoutAndReceivedOrigin500AreDistinct() {
        Logger logger = (Logger) LoggerFactory.getLogger(WriteCoordinator.class);
        Level previous = logger.getLevel();
        logger.setLevel(Level.WARN);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            StatefulCache cache = new StatefulCache();
            byte[] originError = "{\"error\":\"boom\"}".getBytes(StandardCharsets.UTF_8);
            WriteCoordinator timeout = writer(cache, new LocalHotCache(), (method, path, body, contentType) ->
                    OriginResult.unknown("timeout"));
            ResponseEntity<byte[]> unknown = timeout.forward(
                    HttpMethod.PUT, "/api/products/6", new byte[0], "application/json", List.of("product:6"));

            WriteCoordinator received = writer(cache, new LocalHotCache(), (method, path, body, contentType) ->
                    OriginResult.ok(500, "application/json", originError));
            ResponseEntity<byte[]> serverError = received.forward(
                    HttpMethod.POST, "/api/products/6/reviews", new byte[0], "application/json", List.of("reviews:6"));

            assertEquals(504, unknown.getStatusCode().value());
            assertEquals("unknown-timeout", unknown.getHeaders().getFirst("X-Write-Outcome"));
            assertTrue(new String(unknown.getBody(), StandardCharsets.UTF_8).contains("\"reason\":\"timeout\""));
            assertEquals(500, serverError.getStatusCode().value());
            assertEquals("received-5xx", serverError.getHeaders().getFirst("X-Write-Outcome"));
            assertTrue(Arrays.equals(originError, serverError.getBody()));
            assertTrue(cache.events().contains("invalidate:product:6"));
            assertTrue(cache.events().contains("invalidate:reviews:6"));
            assertTrue(appender.list.stream().anyMatch(event ->
                    event.getLevel() == Level.WARN && event.getFormattedMessage().contains("write outcome unknown after transport timeout")));
            assertTrue(appender.list.stream().anyMatch(event ->
                    event.getLevel() == Level.WARN
                            && event.getFormattedMessage().contains("received origin status 500")
                            && event.getFormattedMessage().contains("not a transport timeout")));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }
    }

    @Test
    void connectionLossIsAnUnknownOutcomeAndFences() {
        StatefulCache cache = new StatefulCache();
        WriteCoordinator writes = writer(cache, new LocalHotCache(), (method, path, body, contentType) ->
                OriginResult.unknown("connection"));

        ResponseEntity<byte[]> response = writes.forward(
                HttpMethod.POST, "/api/products/6/reviews", new byte[0], "application/json", List.of("reviews:6"));

        assertEquals(504, response.getStatusCode().value());
        assertEquals("unknown-connection", response.getHeaders().getFirst("X-Write-Outcome"));
        assertTrue(new String(response.getBody(), StandardCharsets.UTF_8).contains("\"reason\":\"connection\""));
        assertTrue(cache.events().contains("invalidate:reviews:6"));
    }

    @Test
    void rejectionDoesNotFenceWhenTheOriginWasNotCalled() {
        StatefulCache cache = new StatefulCache();
        cache.pinGeneration("product:4");
        String generation = cache.generation("product:4");
        WriteCoordinator writes = writer(cache, new LocalHotCache(), (method, path, body, contentType) ->
                OriginResult.rejected());

        ResponseEntity<byte[]> response = writes.forward(
                HttpMethod.PUT, "/api/products/4", new byte[0], "application/json", List.of("product:4"));

        assertEquals(429, response.getStatusCode().value());
        assertEquals("1", response.getHeaders().getFirst("Retry-After"));
        assertEquals(generation, cache.generation("product:4"));
        assertTrue(cache.events().stream().noneMatch(event -> event.startsWith("invalidate:")));
    }

    @Test
    void clientErrorDoesNotFence() {
        StatefulCache cache = new StatefulCache();
        cache.pinGeneration("product:4");
        String generation = cache.generation("product:4");
        byte[] body = "{\"error\":\"price\"}".getBytes(StandardCharsets.UTF_8);
        WriteCoordinator writes = writer(cache, new LocalHotCache(), (method, path, requestBody, contentType) ->
                OriginResult.ok(400, "application/json", body));

        ResponseEntity<byte[]> response = writes.forward(
                HttpMethod.PUT, "/api/products/4", new byte[0], "application/json", List.of("product:4"));

        assertEquals(400, response.getStatusCode().value());
        assertTrue(Arrays.equals(body, response.getBody()));
        assertNull(response.getHeaders().getFirst("X-Write-Outcome"));
        assertEquals(generation, cache.generation("product:4"));
        assertTrue(cache.events().stream().noneMatch(event -> event.startsWith("invalidate:")));
    }

    /**
     * 400 and 404 from catalog writes occur before UPDATE or INSERT. A failed read after UPDATE is 500 and is fenced.
     * 302, 409, and 422 are not produced by those methods. The proxy skips them because they are outside 2xx and 5xx, which is not a pre-mutation guarantee.
     */
    @Test
    void receivedStatusesOutside2xxAnd5xxSkipFencing() {
        for (int status : new int[]{302, 400, 404, 409, 422}) {
            assertFenceSkipped(status);
        }
        for (int status : new int[]{200, 201, 204, 500}) {
            assertFenceApplied(status);
        }
    }

    private static void assertFenceSkipped(int status) {
        StatefulCache cache = new StatefulCache();
        cache.pinGeneration("product:4");
        String generation = cache.generation("product:4");
        WriteCoordinator writes = writer(cache, new LocalHotCache(), (method, path, body, contentType) ->
                OriginResult.ok(status, "application/json", new byte[]{'x'}));

        ResponseEntity<byte[]> response = writes.forward(
                HttpMethod.PUT, "/api/products/4", new byte[0], "application/json", List.of("product:4"));

        assertEquals(status, response.getStatusCode().value());
        assertEquals(generation, cache.generation("product:4"), "status " + status);
        assertTrue(cache.events().stream().noneMatch(event -> event.startsWith("invalidate:")), "status " + status);
    }

    private static void assertFenceApplied(int status) {
        StatefulCache cache = new StatefulCache();
        cache.pinGeneration("product:4");
        String generation = cache.generation("product:4");
        WriteCoordinator writes = writer(cache, new LocalHotCache(), (method, path, body, contentType) ->
                OriginResult.ok(status, "application/json", new byte[]{'x'}));

        ResponseEntity<byte[]> response = writes.forward(
                HttpMethod.PUT, "/api/products/4", new byte[0], "application/json", List.of("product:4"));

        assertEquals(status, response.getStatusCode().value());
        assertNotEquals(generation, cache.generation("product:4"), "status " + status);
        assertTrue(cache.events().contains("invalidate:product:4"), "status " + status);
    }

    @Test
    void timeoutAfterAFenceMakesTheNextReadMiss() {
        StatefulCache cache = new StatefulCache();
        LocalHotCache local = new LocalHotCache();
        cache.preload("product:6", entry(200, "{\"price\":1}", 60_000));
        AtomicInteger reads = new AtomicInteger();
        ReadCoordinator coordinator = reader(cache, local, (method, path, body, contentType) -> {
            reads.incrementAndGet();
            return OriginResult.ok(200, "application/json", "{\"price\":2}".getBytes(StandardCharsets.UTF_8));
        });
        WriteCoordinator writes = writer(cache, local, (method, path, body, contentType) -> OriginResult.unknown("timeout"));

        ResponseEntity<byte[]> write = writes.forward(
                HttpMethod.PUT, "/api/products/6", new byte[0], "application/json", List.of("product:6"));
        ResponseEntity<byte[]> follow = coordinator.read("product", "product:6", "/api/products/6");

        assertEquals(504, write.getStatusCode().value());
        assertEquals("MISS", follow.getHeaders().getFirst("X-Cache"));
        assertEquals(1, reads.get());
    }

    @Test
    void writeTransportFailuresAreClassifiedSeparatelyFromEachOther() {
        assertEquals("timeout", HttpOriginGateway.writeTransportReason(
                new ResourceAccessException("Read timed out", new SocketTimeoutException("Read timed out"))));
        assertEquals("connection", HttpOriginGateway.writeTransportReason(
                new ResourceAccessException("Connection refused", new ConnectException("Connection refused"))));
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

    private static ReadCoordinator coordinator(FakeCache cache, OriginGateway origin) {
        ProxyProperties properties = new ProxyProperties();
        properties.getCoalescing().setMaxWait(Duration.ofSeconds(2));
        return new ReadCoordinator(
                cache,
                new LocalHotCache(),
                new CoalescingEngine(),
                new HotKeyDetector(Duration.ofSeconds(5), () -> 100_000, () -> 0.5, System::currentTimeMillis),
                origin,
                new RuntimeSettings(properties),
                new ProxyMetrics(new SimpleMeterRegistry()),
                properties
        );
    }

    private static CacheEntry entry(int status, String json, long freshForMs) {
        long now = System.currentTimeMillis();
        return new CacheEntry(status, "application/json", json.getBytes(StandardCharsets.UTF_8), now + freshForMs, now + freshForMs + 30_000, false);
    }

    private static final class FakeCache implements ResponseCache {
        private boolean available = true;
        private CacheLookup lookup = CacheLookup.miss();
        private final AtomicInteger stores = new AtomicInteger();
        private final List<String> invalidated = new ArrayList<>();
        private String generation = "1";
        private int generationNumber = 1;

        @Override
        public boolean available() {
            return available;
        }

        @Override
        public CacheLookup lookup(String key) {
            return lookup;
        }

        @Override
        public CacheEntry store(String key, int status, String contentType, byte[] body, CachePolicy policy, boolean hot) {
            stores.incrementAndGet();
            long now = System.currentTimeMillis();
            return new CacheEntry(status, contentType, body, now + 60_000, now + 90_000, status == 404);
        }

        @Override
        public Optional<String> pinGeneration(String key) {
            if (!available) {
                return Optional.empty();
            }
            return Optional.of(generation);
        }

        @Override
        public CacheEntry storeIfGeneration(String key, String expected, int status, String contentType, byte[] body, CachePolicy policy, boolean hot) {
            if (!available || expected == null || !expected.equals(generation)) {
                return null;
            }
            return store(key, status, contentType, body, policy, hot);
        }

        @Override
        public void invalidate(String key) {
            invalidated.add(key);
            generationNumber++;
            generation = Integer.toString(generationNumber);
        }
    }
}
