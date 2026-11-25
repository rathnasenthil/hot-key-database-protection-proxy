package com.hotkey.proxy.core;

import com.hotkey.proxy.cache.LocalHotCache;
import com.hotkey.proxy.cache.ResponseCache;
import com.hotkey.proxy.config.ProxyProperties;
import com.hotkey.proxy.config.RuntimeSettings;
import com.hotkey.proxy.origin.OriginGateway;
import com.hotkey.proxy.origin.OriginResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class WriteCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WriteCoordinator.class);

    private final OriginGateway origin;
    private final ResponseCache cache;
    private final LocalHotCache localCache;
    private final RuntimeSettings settings;
    private final String instanceId;

    public WriteCoordinator(
            OriginGateway origin,
            ResponseCache cache,
            LocalHotCache localCache,
            RuntimeSettings settings,
            ProxyProperties properties
    ) {
        this.origin = origin;
        this.cache = cache;
        this.localCache = localCache;
        this.settings = settings;
        this.instanceId = properties.getInstanceId();
    }

    public ResponseEntity<byte[]> forward(HttpMethod method, String path, byte[] body, String contentType, List<String> keys) {
        OriginResult result = origin.execute(method, path, body, contentType);
        if (result.kind() == OriginResult.Kind.REJECTED) {
            return error(429, "{\"error\":\"origin overloaded\"}", null);
        }
        if (result.kind() == OriginResult.Kind.UNKNOWN || result.kind() == OriginResult.Kind.FAILED) {
            String reason = "timeout".equals(result.reason()) ? "timeout" : "connection";
            log.warn("write outcome unknown after transport {} for {} {}", reason, method, path);
            fence(keys);
            return error(504, "{\"error\":\"origin outcome unknown\",\"reason\":\"" + reason + "\"}", "unknown-" + reason);
        }
        if (result.status() >= 500) {
            log.warn("received origin status {} for write {} {}; not a transport timeout", result.status(), method, path);
            fence(keys);
            return passThrough(result, "received-5xx");
        }
        // 2xx and received 5xx are fenced. A bulkhead 429 is not, because the origin was not called.
        // Catalog writes produce 400 and 404 only before UPDATE or INSERT. A failed read after UPDATE is 500.
        // 302, 409, and 422 are not produced by those write methods. Skipping them follows the status class, not a pre-mutation guarantee.
        if (result.status() >= 200 && result.status() < 300) {
            fence(keys);
        }
        return passThrough(result, null);
    }

    private void fence(List<String> keys) {
        for (String key : keys) {
            cache.invalidate(key);
            localCache.invalidate(key);
        }
    }

    private ResponseEntity<byte[]> passThrough(OriginResult result, String outcome) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.CONTENT_TYPE, result.contentType() == null ? "application/json" : result.contentType());
        headers.set("X-Proxy-Instance", instanceId);
        if (outcome != null) {
            headers.set("X-Write-Outcome", outcome);
        }
        return new ResponseEntity<>(result.body(), headers, result.status());
    }

    private ResponseEntity<byte[]> error(int status, String json, String outcome) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.CONTENT_TYPE, "application/json");
        headers.set("X-Proxy-Instance", instanceId);
        if (outcome != null) {
            headers.set("X-Write-Outcome", outcome);
        }
        if (status == 429) {
            headers.set(HttpHeaders.RETRY_AFTER, Integer.toString(settings.retryAfterSeconds()));
        }
        return new ResponseEntity<>(json.getBytes(StandardCharsets.UTF_8), headers, status);
    }
}
