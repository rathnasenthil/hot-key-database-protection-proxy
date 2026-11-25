package com.hotkey.proxy.origin;

import com.hotkey.proxy.admission.OriginBulkhead;
import com.hotkey.proxy.config.ProxyProperties;
import com.hotkey.proxy.observe.ProxyMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;

@Component
public class HttpOriginGateway implements OriginGateway {

    private static final Logger log = LoggerFactory.getLogger(HttpOriginGateway.class);

    private final RestClient client;
    private final String baseUrl;
    private final OriginBulkhead bulkhead;
    private final ProxyMetrics metrics;

    public HttpOriginGateway(ProxyProperties properties, OriginBulkhead bulkhead, ProxyMetrics metrics) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(500));
        factory.setReadTimeout(Duration.ofSeconds(2));
        this.client = RestClient.builder().requestFactory(factory).build();
        this.baseUrl = stripTrailingSlash(properties.getOriginBaseUrl());
        this.bulkhead = bulkhead;
        this.metrics = metrics;
    }

    @Override
    public boolean preferStale() {
        return bulkhead.preferStale();
    }

    @Override
    public OriginResult execute(HttpMethod method, String path, byte[] body, String contentType) {
        if (!bulkhead.tryEnter()) {
            metrics.rejected();
            return OriginResult.rejected();
        }
        metrics.originQuery();
        try {
            OriginResult result = exchange(method, path, body, contentType);
            bulkhead.record(result.kind() == OriginResult.Kind.OK && result.status() < 500);
            return result;
        } catch (ResourceAccessException exception) {
            bulkhead.record(false);
            return transportFailure(method, path, exception);
        } catch (RuntimeException exception) {
            bulkhead.record(false);
            return transportFailure(method, path, exception);
        } finally {
            bulkhead.leave();
        }
    }

    private OriginResult transportFailure(HttpMethod method, String path, Exception exception) {
        if (method == HttpMethod.PUT || method == HttpMethod.POST) {
            String reason = writeTransportReason(exception);
            log.warn("origin write transport failure ({}) for {} {}", reason, method, path);
            return OriginResult.unknown(reason);
        }
        return OriginResult.failed();
    }

    /**
     * A read timeout and a dropped connection are both unknown write outcomes, and they are logged separately.
     * A received HTTP status never reaches this method.
     */
    public static String writeTransportReason(Throwable exception) {
        for (Throwable current = exception; current != null; current = current.getCause()) {
            if (current instanceof SocketTimeoutException) {
                return "timeout";
            }
        }
        return "connection";
    }

    private OriginResult exchange(HttpMethod method, String path, byte[] body, String contentType) {
        URI uri = URI.create(baseUrl + path);
        RestClient.RequestBodySpec spec = client.method(method)
                .uri(uri)
                .headers(headers -> {
                    if (contentType != null && !contentType.isBlank()) {
                        headers.set(org.springframework.http.HttpHeaders.CONTENT_TYPE, contentType);
                    }
                    headers.setAccept(ListAccept.JSON_AND_ALL);
                });
        if (body != null && body.length > 0 && (method == HttpMethod.POST || method == HttpMethod.PUT)) {
            spec.body(body);
        }
        return spec.exchange((request, response) -> {
            byte[] responseBody = response.getBody().readAllBytes();
            MediaType responseType = response.getHeaders().getContentType();
            String type = responseType == null ? MediaType.APPLICATION_JSON_VALUE : responseType.toString();
            return OriginResult.ok(response.getStatusCode().value(), type, responseBody);
        });
    }

    private static String stripTrailingSlash(String value) {
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static final class ListAccept {
        private static final java.util.List<MediaType> JSON_AND_ALL = java.util.List.of(MediaType.APPLICATION_JSON, MediaType.ALL);
    }
}
