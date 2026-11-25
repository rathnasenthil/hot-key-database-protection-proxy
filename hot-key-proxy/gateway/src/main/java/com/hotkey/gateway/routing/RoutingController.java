package com.hotkey.gateway.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hotkey.gateway.GatewayProperties;
import com.hotkey.gateway.hash.ConsistentHashRing;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
public class RoutingController {

    private static final Logger log = LoggerFactory.getLogger(RoutingController.class);
    private static final Pattern PRODUCT_ID = Pattern.compile("/api/products/(\\d+)");

    private final GatewayProperties properties;
    private final ProxyHealth health;
    private final ObjectMapper mapper;
    private final ConsistentHashRing<String> ring;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(400)).build();
    private final Upstream upstream;

    public RoutingController(GatewayProperties properties, ProxyHealth health, ObjectMapper mapper) {
        this(properties, health, mapper, null);
    }

    RoutingController(GatewayProperties properties, ProxyHealth health, ObjectMapper mapper, Upstream upstream) {
        this.properties = properties;
        this.health = health;
        this.mapper = mapper;
        if (properties.getProxies().isEmpty()) {
            throw new IllegalStateException("gateway.proxies must contain at least one proxy");
        }
        this.ring = new ConsistentHashRing<>(properties.getProxies(), properties.getVirtualNodes(), proxy -> proxy);
        this.upstream = upstream == null ? this::sendHttp : upstream;
    }

    @FunctionalInterface
    interface Upstream {
        ResponseEntity<byte[]> send(String base, HttpMethod method, String path, byte[] body, String contentType) throws IOException;
    }

    @RequestMapping(path = "/api/products/**", method = {org.springframework.web.bind.annotation.RequestMethod.GET, org.springframework.web.bind.annotation.RequestMethod.PUT, org.springframework.web.bind.annotation.RequestMethod.POST})
    public ResponseEntity<byte[]> route(HttpServletRequest request) throws IOException {
        String key = routingKey(request.getRequestURI());
        byte[] body = request.getInputStream().readAllBytes();
        String path = request.getRequestURI() + (request.getQueryString() == null ? "" : "?" + request.getQueryString());
        HttpMethod method = HttpMethod.valueOf(request.getMethod());
        boolean write = method == HttpMethod.PUT || method == HttpMethod.POST;
        boolean attempted = false;
        for (String proxy : ring.owners(key, properties.getProxies().size())) {
            if (!health.up(proxy)) {
                continue;
            }
            attempted = true;
            try {
                return forward(proxy, method, path, body, request.getContentType(), false);
            } catch (RouteException exception) {
                health.markDown(proxy);
                if (write) {
                    log.warn("suppressing write failover after transport failure for {} {}", method, path);
                    return unknownTransport();
                }
            }
        }
        if (write && !attempted) {
            log.warn("rejecting {} {} because every proxy is already down", method, path);
            return noProxyForWrite();
        }
        try {
            return forward(properties.getOriginBaseUrl(), method, path, body, request.getContentType(), true);
        } catch (RouteException exception) {
            if (write) {
                log.warn("write transport failure on origin fallback for {} {}", method, path);
                return unknownTransport();
            }
            byte[] error = "{\"error\":\"no proxy or origin available\"}".getBytes(StandardCharsets.UTF_8);
            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.CONTENT_TYPE, "application/json");
            headers.set("X-Cache", "BYPASS");
            return new ResponseEntity<>(error, headers, HttpStatus.BAD_GATEWAY);
        }
    }

    private static ResponseEntity<byte[]> noProxyForWrite() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.CONTENT_TYPE, "application/json");
        byte[] error = "{\"error\":\"no proxy available\"}".getBytes(StandardCharsets.UTF_8);
        return new ResponseEntity<>(error, headers, HttpStatus.SERVICE_UNAVAILABLE);
    }

    private static ResponseEntity<byte[]> unknownTransport() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.CONTENT_TYPE, "application/json");
        headers.set("X-Write-Outcome", "unknown-transport");
        byte[] error = "{\"error\":\"origin outcome unknown\",\"reason\":\"transport\"}".getBytes(StandardCharsets.UTF_8);
        return new ResponseEntity<>(error, headers, HttpStatus.GATEWAY_TIMEOUT);
    }

    @PostMapping("/api/config/hot-key-threshold")
    public JsonNode updateThreshold(@RequestBody String body) {
        return fanout("/api/config/hot-key-threshold", body);
    }

    @PostMapping("/api/config")
    public JsonNode updateConfig(@RequestBody String body) {
        return fanout("/api/config", body);
    }

    @PostMapping("/api/admin/redis-availability")
    public JsonNode redisAvailability(@RequestBody String body) {
        return fanout("/api/admin/redis-availability", body);
    }

    @GetMapping("/api/admin/hot-keys")
    public JsonNode hotKeys() {
        ArrayNode all = mapper.createArrayNode();
        for (String proxy : properties.getProxies()) {
            try {
                HttpResponse<String> response = http.send(get(proxy + "/api/admin/hot-keys"), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() >= 200 && response.statusCode() < 300 && response.body() != null) {
                    JsonNode node = mapper.readTree(response.body());
                    if (node.isArray()) {
                        node.forEach(all::add);
                    }
                }
            } catch (Exception ignored) {
                health.markDown(proxy);
            }
        }
        return all;
    }

    private JsonNode fanout(String path, String body) {
        ArrayNode results = mapper.createArrayNode();
        for (String proxy : properties.getProxies()) {
            var item = mapper.createObjectNode();
            item.put("proxy", proxy);
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(proxy + path))
                        .timeout(Duration.ofSeconds(2))
                        .header(HttpHeaders.CONTENT_TYPE, "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body == null ? "{}" : body))
                        .build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                item.put("status", response.statusCode());
                if (response.body() != null && !response.body().isBlank()) {
                    item.set("body", mapper.readTree(response.body()));
                }
            } catch (Exception exception) {
                health.markDown(proxy);
                item.put("status", 502);
                item.put("error", exception.getMessage());
            }
            results.add(item);
        }
        if (results.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "no proxies configured");
        }
        return results;
    }

    private ResponseEntity<byte[]> forward(String base, HttpMethod method, String path, byte[] body, String contentType, boolean originFallback) {
        try {
            ResponseEntity<byte[]> response = upstream.send(base, method, path, body, contentType);
            if (originFallback && (response.getHeaders().getFirst("X-Cache") == null)) {
                HttpHeaders headers = new HttpHeaders();
                headers.addAll(response.getHeaders());
                headers.set("X-Cache", "BYPASS");
                return new ResponseEntity<>(response.getBody(), headers, response.getStatusCode());
            }
            return response;
        } catch (IOException exception) {
            throw new RouteException(exception);
        }
    }

    private ResponseEntity<byte[]> sendHttp(String base, HttpMethod method, String path, byte[] body, String contentType) throws IOException {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(Duration.ofSeconds(5));
            if (contentType != null && !contentType.isBlank() && (method == HttpMethod.POST || method == HttpMethod.PUT)) {
                builder.header(HttpHeaders.CONTENT_TYPE, contentType);
            }
            if (method == HttpMethod.POST || method == HttpMethod.PUT) {
                builder.method(method.name(), HttpRequest.BodyPublishers.ofByteArray(body == null ? new byte[0] : body));
            } else {
                builder.GET();
            }
            HttpResponse<byte[]> upstreamResponse = http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            HttpHeaders headers = new HttpHeaders();
            upstreamResponse.headers().firstValue("content-type")
                    .ifPresentOrElse(value -> headers.set(HttpHeaders.CONTENT_TYPE, value), () -> headers.set(HttpHeaders.CONTENT_TYPE, "application/json"));
            copy(upstreamResponse, headers, "x-cache");
            copy(upstreamResponse, headers, "x-hot-key");
            copy(upstreamResponse, headers, "retry-after");
            copy(upstreamResponse, headers, "x-proxy-instance");
            copy(upstreamResponse, headers, "x-write-outcome");
            headers.set("X-Proxy-Route", base);
            return new ResponseEntity<>(upstreamResponse.body(), headers, upstreamResponse.statusCode());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException(exception);
        } catch (RuntimeException exception) {
            throw new IOException(exception);
        }
    }

    private static void copy(HttpResponse<?> response, HttpHeaders headers, String name) {
        response.headers().firstValue(name).ifPresent(value -> headers.set(name, value));
    }

    private static HttpRequest get(String url) {
        return HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build();
    }

    public static String routingKey(String uri) {
        Matcher matcher = PRODUCT_ID.matcher(uri);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return uri;
    }

    private static final class RouteException extends RuntimeException {
        private RouteException(Throwable cause) {
            super(cause);
        }
    }
}
