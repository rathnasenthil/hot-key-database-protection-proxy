package com.hotkey.proxy.api;

import com.hotkey.proxy.core.ReadCoordinator;
import com.hotkey.proxy.core.WriteCoordinator;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
public class CatalogProxyController {

    private final ReadCoordinator reads;
    private final WriteCoordinator writes;

    public CatalogProxyController(ReadCoordinator reads, WriteCoordinator writes) {
        this.reads = reads;
        this.writes = writes;
    }

    @GetMapping("/api/products/{id}")
    public ResponseEntity<byte[]> product(@PathVariable long id) {
        return reads.read("product", "product:" + id, "/api/products/" + id);
    }

    @GetMapping("/api/products/{id}/reviews")
    public ResponseEntity<byte[]> reviews(@PathVariable long id) {
        return reads.read("reviews", "reviews:" + id, "/api/products/" + id + "/reviews");
    }

    @PutMapping("/api/products/{id}")
    public ResponseEntity<byte[]> update(@PathVariable long id, HttpServletRequest request) throws IOException {
        return writes.forward(
                HttpMethod.PUT,
                "/api/products/" + id,
                request.getInputStream().readAllBytes(),
                request.getContentType(),
                List.of("product:" + id)
        );
    }

    @PostMapping("/api/products/{id}/reviews")
    public ResponseEntity<byte[]> addReview(@PathVariable long id, HttpServletRequest request) throws IOException {
        return writes.forward(
                HttpMethod.POST,
                "/api/products/" + id + "/reviews",
                request.getInputStream().readAllBytes(),
                request.getContentType(),
                List.of("reviews:" + id)
        );
    }
}
