package com.hotkey.gateway.routing;

import com.hotkey.gateway.GatewayProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ProxyHealth {

    private static final Logger log = LoggerFactory.getLogger(ProxyHealth.class);

    private final GatewayProperties properties;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();
    private final Set<String> down = ConcurrentHashMap.newKeySet();

    public ProxyHealth(GatewayProperties properties) {
        this.properties = properties;
    }

    public boolean up(String proxy) {
        return !down.contains(proxy);
    }

    public void markDown(String proxy) {
        if (down.add(proxy)) {
            log.warn("Proxy {} marked down", proxy);
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void probe() {
        for (String proxy : properties.getProxies()) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(proxy + "/actuator/health"))
                        .timeout(Duration.ofMillis(500))
                        .GET()
                        .build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                boolean healthy = response.statusCode() == 200 && response.body() != null && response.body().contains("\"status\":\"UP\"");
                if (healthy) {
                    if (down.remove(proxy)) {
                        log.info("Proxy {} recovered", proxy);
                    }
                } else {
                    markDown(proxy);
                }
            } catch (Exception exception) {
                markDown(proxy);
            }
        }
    }
}
