package com.hotkey.gateway.routing;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hotkey.gateway.GatewayProperties;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingControllerTest {

    @Test
    void productRequestsHashOnTheProductId() {
        assertEquals("42", RoutingController.routingKey("/api/products/42"));
        assertEquals("42", RoutingController.routingKey("/api/products/42/reviews"));
        assertEquals("/api/stats", RoutingController.routingKey("/api/stats"));
    }

    @Test
    void putTransportFailureDoesNotTryAnotherProxyOrTheOrigin() throws Exception {
        assertWriteDoesNotFailOver("PUT");
    }

    @Test
    void postTransportFailureDoesNotTryAnotherProxyOrTheOrigin() throws Exception {
        assertWriteDoesNotFailOver("POST");
    }

    @Test
    void getTransportFailureTriesTheNextProxy() throws Exception {
        List<String> calls = new ArrayList<>();
        RoutingController controller = controller((base, method, path, body, contentType) -> {
            calls.add(method.name() + " " + base);
            if (calls.size() == 1) {
                throw new IOException("read timed out");
            }
            return ResponseEntity.ok(new byte[]{'{', '}'});
        });

        ResponseEntity<byte[]> response = controller.route(request("GET"));

        assertEquals(200, response.getStatusCode().value());
        assertEquals(2, calls.size());
        assertTrue(calls.stream().noneMatch(call -> call.contains("origin")));
    }

    @Test
    void getTransportFailureCanStillReachTheOrigin() throws Exception {
        List<String> calls = new ArrayList<>();
        RoutingController controller = controller((base, method, path, body, contentType) -> {
            calls.add(base);
            throw new IOException("connection reset");
        });

        ResponseEntity<byte[]> response = controller.route(request("GET"));

        assertEquals(502, response.getStatusCode().value());
        assertEquals(3, calls.size());
        assertTrue(calls.get(2).contains("origin"));
    }

    @Test
    void completedWriteResponseIsNotRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RoutingController controller = controller((base, method, path, body, contentType) -> {
            calls.incrementAndGet();
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Write-Outcome", "received-5xx");
            return new ResponseEntity<>("{\"error\":\"boom\"}".getBytes(StandardCharsets.UTF_8), headers, 500);
        });

        ResponseEntity<byte[]> response = controller.route(request("PUT"));

        assertEquals(1, calls.get());
        assertEquals(500, response.getStatusCode().value());
        assertEquals("received-5xx", response.getHeaders().getFirst("X-Write-Outcome"));
    }

    @Test
    void newPutWithEveryProxyDownIsRejectedBeforeTheOrigin() throws Exception {
        assertNewWriteWithEveryProxyDownIsRejected("PUT");
    }

    @Test
    void newPostWithEveryProxyDownIsRejectedBeforeTheOrigin() throws Exception {
        assertNewWriteWithEveryProxyDownIsRejected("POST");
    }

    @Test
    void getWithEveryProxyDownStillReachesTheOrigin() throws Exception {
        List<String> calls = new ArrayList<>();
        RoutingController controller = downController(calls);

        ResponseEntity<byte[]> response = controller.route(request("GET"));

        assertEquals(200, response.getStatusCode().value());
        assertEquals(List.of("GET http://origin:8081"), calls);
        assertEquals("BYPASS", response.getHeaders().getFirst("X-Cache"));
    }

    /**
     * Both proxies are already down, so this request has not been sent.
     * That is separate from a transport failure after a proxy attempt, which returns 504 and does not continue.
     */
    private static void assertNewWriteWithEveryProxyDownIsRejected(String method) throws Exception {
        List<String> calls = new ArrayList<>();
        RoutingController controller = downController(calls);

        ResponseEntity<byte[]> response = controller.route(request(method));

        assertEquals(503, response.getStatusCode().value());
        assertTrue(calls.isEmpty());
        assertTrue(new String(response.getBody(), StandardCharsets.UTF_8).contains("\"error\":\"no proxy available\""));
    }

    private static RoutingController downController(List<String> calls) {
        GatewayProperties properties = new GatewayProperties();
        properties.setProxies(List.of("http://proxy-a:8080", "http://proxy-b:8080"));
        properties.setOriginBaseUrl("http://origin:8081");
        ProxyHealth health = new ProxyHealth(properties);
        health.markDown("http://proxy-a:8080");
        health.markDown("http://proxy-b:8080");
        return new RoutingController(properties, health, new ObjectMapper(), (base, httpMethod, path, body, contentType) -> {
            calls.add(httpMethod.name() + " " + base);
            return ResponseEntity.ok(new byte[]{'{', '}'});
        });
    }

    private static void assertWriteDoesNotFailOver(String method) throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RoutingController.class);
        Level previous = logger.getLevel();
        logger.setLevel(Level.WARN);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            List<String> calls = new ArrayList<>();
            RoutingController controller = controller((base, httpMethod, path, body, contentType) -> {
                calls.add(httpMethod.name() + " " + base);
                throw new IOException("read timed out");
            });

            ResponseEntity<byte[]> response = controller.route(request(method));

            assertEquals(504, response.getStatusCode().value());
            assertEquals(1, calls.size());
            assertTrue(calls.get(0).startsWith(method + " http://proxy-"));
            assertEquals("unknown-transport", response.getHeaders().getFirst("X-Write-Outcome"));
            assertTrue(new String(response.getBody(), StandardCharsets.UTF_8).contains("\"reason\":\"transport\""));
            assertTrue(appender.list.stream().anyMatch(event ->
                    event.getLevel() == Level.WARN
                            && event.getFormattedMessage().contains("suppressing write failover after transport failure")));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }
    }

    private static RoutingController controller(RoutingController.Upstream upstream) {
        GatewayProperties properties = new GatewayProperties();
        properties.setProxies(List.of("http://proxy-a:8080", "http://proxy-b:8080"));
        properties.setOriginBaseUrl("http://origin:8081");
        return new RoutingController(properties, new ProxyHealth(properties), new ObjectMapper(), upstream);
    }

    private static MockHttpServletRequest request(String method) {
        MockHttpServletRequest request = new MockHttpServletRequest(method, "/api/products/8");
        request.setContent("{}".getBytes(StandardCharsets.UTF_8));
        return request;
    }
}
