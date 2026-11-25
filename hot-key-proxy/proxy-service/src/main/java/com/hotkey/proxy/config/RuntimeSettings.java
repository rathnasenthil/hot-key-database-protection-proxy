package com.hotkey.proxy.config;

import com.hotkey.proxy.cache.CachePolicy;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class RuntimeSettings {

    private final ProxyProperties properties;
    private final AtomicReference<Double> hotKeyThreshold = new AtomicReference<>();
    private final AtomicReference<Duration> coalesceMaxWait = new AtomicReference<>();
    private final AtomicReference<Map<String, CachePolicy>> policies = new AtomicReference<>();

    public RuntimeSettings(ProxyProperties properties) {
        this.properties = properties;
        hotKeyThreshold.set(properties.getHotKey().getThreshold());
        coalesceMaxWait.set(properties.getCoalescing().getMaxWait());
        policies.set(copyPolicies(properties.getPolicies()));
    }

    public double hotKeyThreshold() {
        return hotKeyThreshold.get();
    }

    public void setHotKeyThreshold(double threshold) {
        if (threshold <= 0) {
            throw new IllegalArgumentException("threshold must be positive");
        }
        hotKeyThreshold.set(threshold);
    }

    public Duration coalesceMaxWait() {
        return coalesceMaxWait.get();
    }

    public void setCoalesceMaxWait(Duration maxWait) {
        if (maxWait == null || maxWait.isNegative() || maxWait.isZero()) {
            throw new IllegalArgumentException("coalesce max wait must be positive");
        }
        coalesceMaxWait.set(maxWait);
    }

    public Duration hotKeyWindow() {
        return properties.getHotKey().getWindow();
    }

    public double demoteFraction() {
        return properties.getHotKey().getDemoteFraction();
    }

    public Duration negativeTtl() {
        return properties.getNegativeTtl();
    }

    public double ttlJitterFraction() {
        return properties.getTtlJitterFraction();
    }

    public int retryAfterSeconds() {
        return properties.getRetryAfterSeconds();
    }

    public CachePolicy policy(String keyType) {
        CachePolicy policy = policies.get().get(keyType);
        if (policy != null) {
            return policy;
        }
        CachePolicy fallback = policies.get().get("product");
        return fallback != null
                ? fallback
                : new CachePolicy(Duration.ofSeconds(60), Duration.ofSeconds(30), Duration.ofSeconds(300));
    }

    public void replacePolicy(String keyType, CachePolicy policy) {
        Map<String, CachePolicy> updated = new LinkedHashMap<>(policies.get());
        updated.put(keyType, policy);
        policies.set(Map.copyOf(updated));
    }

    public Map<String, CachePolicy> policies() {
        return policies.get();
    }

    private static Map<String, CachePolicy> copyPolicies(Map<String, ProxyProperties.Policy> source) {
        Map<String, CachePolicy> copy = new LinkedHashMap<>();
        source.forEach((name, policy) -> copy.put(name, new CachePolicy(
                policy.getTtl(),
                policy.getStaleWindow(),
                policy.getHotTtl()
        )));
        return Map.copyOf(copy);
    }
}
