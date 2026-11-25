package com.hotkey.proxy.observe;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class ProxyMetrics {

    private final MeterRegistry registry;

    public ProxyMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void cacheResult(String result, long nanos) {
        registry.counter("proxy.cache.requests", "result", result).increment();
        Timer.builder("proxy.request.latency")
                .tag("result", result)
                .publishPercentileHistogram()
                .register(registry)
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    public void coalesced() {
        registry.counter("proxy.coalesced.requests").increment();
    }

    public void originQuery() {
        registry.counter("proxy.origin.queries").increment();
    }

    public void rejected() {
        registry.counter("proxy.admission.rejections").increment();
    }
}
