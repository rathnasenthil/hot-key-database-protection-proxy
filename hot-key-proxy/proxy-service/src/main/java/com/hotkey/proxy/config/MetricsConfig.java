package com.hotkey.proxy.config;

import com.hotkey.proxy.admission.OriginBulkhead;
import com.hotkey.proxy.cache.RedisCacheClient;
import com.hotkey.proxy.hotkey.HotKeyDetector;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    MeterBinder proxyGauges(OriginBulkhead bulkhead, HotKeyDetector hotKeys, RedisCacheClient redis) {
        return registry -> {
            Gauge.builder("proxy.origin.concurrency", bulkhead, gate -> gate.inUse()).register(registry);
            Gauge.builder("proxy.hot.keys", hotKeys, detector -> detector.promotedCount()).register(registry);
            Gauge.builder("proxy.redis.nodes.up", redis, cache -> cache.upCount()).register(registry);
            Gauge.builder("proxy.redis.nodes", redis, cache -> cache.nodeCount()).register(registry);
        };
    }
}
