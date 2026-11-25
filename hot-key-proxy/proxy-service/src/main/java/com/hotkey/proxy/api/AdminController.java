package com.hotkey.proxy.api;

import com.hotkey.proxy.cache.CachePolicy;
import com.hotkey.proxy.cache.RedisCacheClient;
import com.hotkey.proxy.config.RuntimeSettings;
import com.hotkey.proxy.hotkey.HotKeyDetector;
import com.hotkey.proxy.hotkey.HotKeyInfo;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
public class AdminController {

    private final HotKeyDetector hotKeys;
    private final RuntimeSettings settings;
    private final RedisCacheClient redis;

    public AdminController(HotKeyDetector hotKeys, RuntimeSettings settings, RedisCacheClient redis) {
        this.hotKeys = hotKeys;
        this.settings = settings;
        this.redis = redis;
    }

    @GetMapping("/api/admin/hot-keys")
    public List<HotKeyInfo> hotKeys() {
        return hotKeys.top(20);
    }

    @GetMapping("/api/admin/config")
    public Map<String, Object> config() {
        Map<String, Object> policies = new LinkedHashMap<>();
        settings.policies().forEach((name, policy) -> policies.put(name, policyView(policy)));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("hotKeyThreshold", settings.hotKeyThreshold());
        body.put("windowSeconds", settings.hotKeyWindow().toSeconds());
        body.put("coalesceMaxWaitMs", settings.coalesceMaxWait().toMillis());
        body.put("redisAvailable", redis.available());
        body.put("redisForcedAvailable", redis.isForcedAvailable());
        body.put("policies", policies);
        return body;
    }

    @PostMapping("/api/config/hot-key-threshold")
    public Map<String, Object> updateThreshold(@RequestBody Map<String, Object> body) {
        Object raw = body.get("threshold");
        if (raw == null) {
            raw = body.get("thresholdRps");
        }
        if (!(raw instanceof Number number)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "threshold is required");
        }
        settings.setHotKeyThreshold(number.doubleValue());
        return Map.of(
                "threshold", settings.hotKeyThreshold(),
                "windowSeconds", settings.hotKeyWindow().toSeconds()
        );
    }

    @PostMapping("/api/config")
    public Map<String, Object> updateConfig(@RequestBody Map<String, Object> body) {
        if (body.get("hotKeyThreshold") instanceof Number number) {
            settings.setHotKeyThreshold(number.doubleValue());
        }
        if (body.get("coalesceMaxWaitMs") instanceof Number number) {
            settings.setCoalesceMaxWait(Duration.ofMillis(number.longValue()));
        }
        Object policies = body.get("policies");
        if (policies instanceof Map<?, ?> policyMap) {
            for (Map.Entry<?, ?> entry : policyMap.entrySet()) {
                if (entry.getKey() instanceof String name && entry.getValue() instanceof Map<?, ?> values) {
                    CachePolicy current = settings.policy(name);
                    long ttl = number(values, "ttlMs", current.ttl().toMillis());
                    long stale = number(values, "staleWindowMs", current.staleWindow().toMillis());
                    long hot = number(values, "hotTtlMs", current.hotTtl().toMillis());
                    settings.replacePolicy(name, new CachePolicy(
                            Duration.ofMillis(ttl),
                            Duration.ofMillis(stale),
                            Duration.ofMillis(hot)
                    ));
                }
            }
        }
        return config();
    }

    @PostMapping("/api/admin/redis-availability")
    public Map<String, Object> redisAvailability(@RequestBody Map<String, Object> body) {
        Object raw = body.get("available");
        if (!(raw instanceof Boolean available)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "available boolean is required");
        }
        redis.setForcedAvailable(available);
        return Map.of("available", redis.isForcedAvailable());
    }

    private static Map<String, Object> policyView(CachePolicy policy) {
        return Map.of(
                "ttlMs", policy.ttl().toMillis(),
                "staleWindowMs", policy.staleWindow().toMillis(),
                "hotTtlMs", policy.hotTtl().toMillis()
        );
    }

    private static long number(Map<?, ?> values, String key, long fallback) {
        Object raw = values.get(key);
        if (raw instanceof Number number) {
            return number.longValue();
        }
        return fallback;
    }
}
