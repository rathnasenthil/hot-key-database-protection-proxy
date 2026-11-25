package com.hotkey.gateway.sim;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hotkey.gateway.GatewayProperties;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class WorkloadRunner {

    private static final Set<String> WORKLOADS = Set.of("uniform", "hotkey", "moving", "redis-failure", "overload");
    private static final long[] BOUNDS_MS = {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000};

    private final GatewayProperties properties;
    private final ObjectMapper mapper;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean cancel = new AtomicBoolean();
    private volatile Map<String, Object> current = Map.of("running", false);
    private volatile Map<String, Object> lastBaseline;
    private volatile Map<String, Object> lastProtected;

    public WorkloadRunner(GatewayProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
    }

    public synchronized Map<String, Object> start(SimulationController.StartRequest request) {
        if (running.get()) {
            throw new IllegalStateException("a workload is already running");
        }
        String workload = request.workload() == null ? "" : request.workload().trim().toLowerCase(Locale.ROOT);
        String mode = request.mode() == null ? "" : request.mode().trim().toLowerCase(Locale.ROOT);
        int duration = request.durationSeconds() == null ? 30 : request.durationSeconds();
        int concurrency = request.concurrency() == null ? 200 : request.concurrency();
        if (!WORKLOADS.contains(workload)) {
            throw new IllegalArgumentException("workload must be one of " + WORKLOADS);
        }
        if (!mode.equals("baseline") && !mode.equals("protected")) {
            throw new IllegalArgumentException("mode must be baseline or protected");
        }
        if (duration < 5 || duration > 180) {
            throw new IllegalArgumentException("durationSeconds must be between 5 and 180");
        }
        if (concurrency < 1 || concurrency > 10_000) {
            throw new IllegalArgumentException("concurrency must be between 1 and 10000");
        }
        running.set(true);
        cancel.set(false);
        current = Map.of(
                "running", true,
                "workload", workload,
                "mode", mode,
                "durationSeconds", duration,
                "concurrency", concurrency,
                "startedAt", Instant.now().toString()
        );
        Thread.startVirtualThread(() -> execute(workload, mode, duration, concurrency));
        return new LinkedHashMap<>(current);
    }

    public Map<String, Object> stop() {
        cancel.set(true);
        return status();
    }

    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("running", running.get());
        body.put("current", current);
        body.put("lastBaseline", lastBaseline);
        body.put("lastProtected", lastProtected);
        body.put("live", liveMetrics());
        body.put("productCount", properties.getProductCount());
        return body;
    }

    private void execute(String workload, String mode, int durationSeconds, int concurrency) {
        Histogram all = new Histogram();
        Histogram hot = new Histogram();
        Histogram other = new Histogram();
        AtomicLong sent = new AtomicLong();
        AtomicLong ok = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        AtomicLong observedConnections = new AtomicLong();
        String base = "baseline".equals(mode) ? properties.getOriginBaseUrl() : properties.getSelfBaseUrl();
        boolean protect = "protected".equals(mode);
        Map<String, Double> before = protect ? proxyCounters() : Map.of();
        long startedMs = System.currentTimeMillis();
        long deadline = startedMs + durationSeconds * 1000L;
        Semaphore inflight = new Semaphore(concurrency);
        AtomicBoolean sampling = new AtomicBoolean(true);
        boolean redisCut = false;
        Thread.startVirtualThread(() -> sampleConnections(sampling, observedConnections));

        try {
            boolean redisRestored = false;
            while (!cancel.get() && System.currentTimeMillis() < deadline) {
                long now = System.currentTimeMillis();
                if (protect && "redis-failure".equals(workload) && !redisCut && now - startedMs > durationSeconds * 200L) {
                    setRedisAvailable(false);
                    redisCut = true;
                }
                if (protect && redisCut && !redisRestored && now - startedMs > durationSeconds * 700L) {
                    setRedisAvailable(true);
                    redisRestored = true;
                }
                if (!inflight.tryAcquire(50, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                    continue;
                }
                long hotId = hotId(workload);
                Thread.startVirtualThread(() -> {
                    try {
                        oneRequest(base, workload, hotId, all, hot, other, sent, ok, rejected, errors);
                    } finally {
                        inflight.release();
                    }
                });
            }
            long drainUntil = System.currentTimeMillis() + 8_000;
            while (inflight.availablePermits() < concurrency && System.currentTimeMillis() < drainUntil) {
                Thread.sleep(25);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            if (redisCut) {
                setRedisAvailable(true);
            }
            sampling.set(false);
            long elapsedMs = Math.max(1, System.currentTimeMillis() - startedMs);
            Map<String, Double> after = protect ? proxyCounters() : Map.of();
            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("workload", workload);
            snapshot.put("mode", mode);
            snapshot.put("durationSeconds", elapsedMs / 1000.0);
            snapshot.put("requests", sent.get());
            snapshot.put("success", ok.get());
            snapshot.put("rejected", rejected.get());
            snapshot.put("errors", errors.get());
            snapshot.put("clientP95Ms", all.percentile(0.95));
            snapshot.put("clientP99Ms", all.percentile(0.99));
            snapshot.put("hotP99Ms", hot.percentile(0.99));
            snapshot.put("otherP99Ms", other.percentile(0.99));
            snapshot.put("maxOriginConnections", observedConnections.get() / 1000.0);
            if (protect) {
                double seconds = elapsedMs / 1000.0;
                snapshot.put("proxyOriginQueries", delta(after, before, "origin"));
                snapshot.put("proxyOriginQps", delta(after, before, "origin") / seconds);
                snapshot.put("coalesced", delta(after, before, "coalesced"));
                snapshot.put("admissionRejections", delta(after, before, "rejected"));
                snapshot.put("cacheHitRatio", hitRatio(before, after));
            }
            snapshot.put("finishedAt", Instant.now().toString());
            if ("baseline".equals(mode)) {
                lastBaseline = snapshot;
            } else {
                lastProtected = snapshot;
            }
            current = snapshot;
            running.set(false);
        }
    }

    private void oneRequest(
            String base,
            String workload,
            long hotId,
            Histogram all,
            Histogram hot,
            Histogram other,
            AtomicLong sent,
            AtomicLong ok,
            AtomicLong rejected,
            AtomicLong errors
    ) {
        int products = Math.max(1, properties.getProductCount());
        long id = ThreadLocalRandom.current().nextLong(1, products + 1L);
        String path;
        HttpRequest.Builder builder;
        boolean hotRequest = false;
        double roll = ThreadLocalRandom.current().nextDouble();
        try {
            if ("uniform".equals(workload)) {
                if (roll < 0.90) {
                    path = "/api/products/" + id;
                    builder = HttpRequest.newBuilder(URI.create(base + path)).GET();
                } else if (roll < 0.95) {
                    path = "/api/products/" + id + "/reviews";
                    builder = HttpRequest.newBuilder(URI.create(base + path)).GET();
                } else {
                    path = "/api/products/" + id + "/reviews";
                    builder = HttpRequest.newBuilder(URI.create(base + path))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"userId\":1,\"rating\":5,\"comment\":\"sim\"}"));
                }
            } else if ("overload".equals(workload)) {
                path = "/api/products/" + id;
                builder = HttpRequest.newBuilder(URI.create(base + path)).GET();
            } else {
                hotRequest = roll < 0.85;
                long target = hotRequest ? hotId : id;
                if (target == hotId) {
                    hotRequest = true;
                }
                path = "/api/products/" + target;
                builder = HttpRequest.newBuilder(URI.create(base + path)).GET();
            }
            HttpRequest request = builder.timeout(Duration.ofSeconds(5)).build();
            long start = System.nanoTime();
            HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            sent.incrementAndGet();
            all.record(elapsedMs);
            if (hotRequest) {
                hot.record(elapsedMs);
            } else {
                other.record(elapsedMs);
            }
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                ok.incrementAndGet();
            } else if (status == 429) {
                rejected.incrementAndGet();
            } else {
                errors.incrementAndGet();
            }
        } catch (Exception exception) {
            sent.incrementAndGet();
            errors.incrementAndGet();
            all.record(5000);
        }
    }

    private long hotId(String workload) {
        if ("moving".equals(workload)) {
            long slot = System.currentTimeMillis() / 30_000;
            return (Math.floorMod(slot, 20)) + 1;
        }
        if ("uniform".equals(workload) || "overload".equals(workload)) {
            return 0;
        }
        return 1;
    }

    private void sampleConnections(AtomicBoolean sampling, AtomicLong observedMillisScaled) {
        while (sampling.get()) {
            double value = scrapeGauge(properties.getOriginBaseUrl() + "/actuator/prometheus", "hikaricp_connections_active");
            long scaled = (long) Math.round(value * 1000);
            observedMillisScaled.accumulateAndGet(scaled, Math::max);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void setRedisAvailable(boolean available) {
        String json = "{\"available\":" + available + "}";
        for (String proxy : properties.getProxies()) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(proxy + "/api/admin/redis-availability"))
                        .timeout(Duration.ofSeconds(2))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();
                http.send(request, HttpResponse.BodyHandlers.discarding());
            } catch (Exception ignored) {
                // The real container-stop test is performed by the k6 script.
            }
        }
    }

    private Map<String, Double> proxyCounters() {
        double origin = 0;
        double coalesced = 0;
        double rejected = 0;
        double served = 0;
        double total = 0;
        for (String proxy : properties.getProxies()) {
            List<Sample> samples = parse(scrape(proxy + "/api/metrics"));
            origin += sum(samples, "proxy_origin_queries_total");
            coalesced += sum(samples, "proxy_coalesced_requests_total");
            rejected += sum(samples, "proxy_admission_rejections_total");
            double proxyServed = sumLabel(samples, "proxy_cache_requests_total", "result", "hit")
                    + sumLabel(samples, "proxy_cache_requests_total", "result", "stale")
                    + sumLabel(samples, "proxy_cache_requests_total", "result", "coalesced");
            served += proxyServed;
            total += proxyServed
                    + sumLabel(samples, "proxy_cache_requests_total", "result", "miss")
                    + sumLabel(samples, "proxy_cache_requests_total", "result", "bypass");
        }
        Map<String, Double> values = new LinkedHashMap<>();
        values.put("origin", origin);
        values.put("coalesced", coalesced);
        values.put("rejected", rejected);
        values.put("hit", served);
        values.put("useful", total);
        return values;
    }

    private static double delta(Map<String, Double> after, Map<String, Double> before, String key) {
        return Math.max(0, after.getOrDefault(key, 0.0) - before.getOrDefault(key, 0.0));
    }

    private static Double hitRatio(Map<String, Double> before, Map<String, Double> after) {
        double hits = delta(after, before, "hit");
        double total = delta(after, before, "useful");
        if (total <= 0) {
            return null;
        }
        return hits / total;
    }

    private Map<String, Object> liveMetrics() {
        Map<String, Object> live = new LinkedHashMap<>();
        live.put("originRps", prom("sum(rate(http_server_requests_seconds_count{application=\"origin-service\",uri=~\"/api/products.*\"}[1m]))"));
        live.put("proxyOriginRps", prom("sum(rate(proxy_origin_queries_total[1m]))"));
        live.put("cacheHitRatio", prom("sum(rate(proxy_cache_requests_total{result=~\"hit|stale|coalesced\"}[1m])) / sum(rate(proxy_cache_requests_total{result=~\"hit|stale|coalesced|miss|bypass\"}[1m]))"));
        live.put("proxyP99", prom("histogram_quantile(0.99, sum by (le) (rate(proxy_request_latency_seconds_bucket[1m])))"));
        live.put("originP99", prom("histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{application=\"origin-service\"}[1m])))"));
        live.put("dbConnections", prom("sum(hikaricp_connections_active{application=\"origin-service\"})"));
        live.put("coalescedRps", prom("sum(rate(proxy_coalesced_requests_total[1m]))"));
        live.put("rejectedRps", prom("sum(rate(proxy_admission_rejections_total[1m]))"));
        return live;
    }

    private Double prom(String query) {
        try {
            String url = properties.getPrometheusUrl() + "/api/v1/query?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200 || response.body() == null) {
                return null;
            }
            JsonNode result = mapper.readTree(response.body()).path("data").path("result");
            if (!result.isArray() || result.isEmpty()) {
                return null;
            }
            double sum = 0;
            for (JsonNode row : result) {
                JsonNode value = row.path("value");
                if (value.isArray() && value.size() > 1) {
                    sum += value.get(1).asDouble();
                }
            }
            if (Double.isNaN(sum) || Double.isInfinite(sum)) {
                return null;
            }
            return sum;
        } catch (Exception exception) {
            return null;
        }
    }

    private double scrapeGauge(String url, String name) {
        return sum(parse(scrape(url)), name);
    }

    private String scrape(String url) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body();
            }
        } catch (Exception ignored) {
            return "";
        }
        return "";
    }

    static List<Sample> parse(String text) {
        List<Sample> samples = new ArrayList<>();
        if (text == null || text.isBlank()) {
            return samples;
        }
        for (String raw : text.split("\n")) {
            String line = raw.trim();
            if (line.isEmpty() || line.charAt(0) == '#') {
                continue;
            }
            int space = line.lastIndexOf(' ');
            if (space <= 0) {
                continue;
            }
            double value;
            try {
                value = Double.parseDouble(line.substring(space + 1).trim());
            } catch (NumberFormatException exception) {
                continue;
            }
            String left = line.substring(0, space).trim();
            int brace = left.indexOf('{');
            if (brace < 0) {
                samples.add(new Sample(left, Map.of(), value));
            } else if (left.endsWith("}")) {
                samples.add(new Sample(left.substring(0, brace), parseLabels(left.substring(brace + 1, left.length() - 1)), value));
            }
        }
        return samples;
    }

    private static Map<String, String> parseLabels(String raw) {
        Map<String, String> labels = new LinkedHashMap<>();
        int index = 0;
        while (index < raw.length()) {
            int equals = raw.indexOf('=', index);
            if (equals < 0 || equals + 1 >= raw.length() || raw.charAt(equals + 1) != '"') {
                break;
            }
            String key = raw.substring(index, equals).trim();
            int cursor = equals + 2;
            StringBuilder value = new StringBuilder();
            while (cursor < raw.length()) {
                char current = raw.charAt(cursor);
                if (current == '\\' && cursor + 1 < raw.length()) {
                    value.append(raw.charAt(cursor + 1));
                    cursor += 2;
                    continue;
                }
                if (current == '"') {
                    break;
                }
                value.append(current);
                cursor++;
            }
            labels.put(key, value.toString());
            index = cursor + 1;
            if (index < raw.length() && raw.charAt(index) == ',') {
                index++;
            }
        }
        return labels;
    }

    private static double sum(List<Sample> samples, String name) {
        double total = 0;
        for (Sample sample : samples) {
            if (name.equals(sample.name())) {
                total += sample.value();
            }
        }
        return total;
    }

    private static double sumLabel(List<Sample> samples, String name, String label, String expected) {
        double total = 0;
        for (Sample sample : samples) {
            if (name.equals(sample.name()) && expected.equals(sample.labels().get(label))) {
                total += sample.value();
            }
        }
        return total;
    }

    private record Sample(String name, Map<String, String> labels, double value) {
    }

    private static final class Histogram {
        private final AtomicLong[] counts = new AtomicLong[BOUNDS_MS.length + 1];
        private final AtomicLong total = new AtomicLong();

        private Histogram() {
            for (int i = 0; i < counts.length; i++) {
                counts[i] = new AtomicLong();
            }
        }

        void record(long millis) {
            total.incrementAndGet();
            int index = BOUNDS_MS.length;
            for (int i = 0; i < BOUNDS_MS.length; i++) {
                if (millis <= BOUNDS_MS[i]) {
                    index = i;
                    break;
                }
            }
            counts[index].incrementAndGet();
        }

        double percentile(double p) {
            long count = total.get();
            if (count == 0) {
                return 0;
            }
            long target = (long) Math.ceil(p * count);
            long seen = 0;
            for (int i = 0; i < counts.length; i++) {
                seen += counts[i].get();
                if (seen >= target) {
                    return i < BOUNDS_MS.length ? BOUNDS_MS[i] : BOUNDS_MS[BOUNDS_MS.length - 1];
                }
            }
            return BOUNDS_MS[BOUNDS_MS.length - 1];
        }
    }
}
