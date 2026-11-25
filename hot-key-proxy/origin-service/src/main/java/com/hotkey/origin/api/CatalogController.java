package com.hotkey.origin.api;

import com.hotkey.origin.catalog.CatalogService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class CatalogController {

    private final CatalogService catalog;

    public CatalogController(CatalogService catalog) {
        this.catalog = catalog;
    }

    @GetMapping("/products/{id}")
    public ProductResponse getProduct(@PathVariable long id) {
        return catalog.getProduct(id);
    }

    @GetMapping("/products/{id}/reviews")
    public List<ReviewResponse> getReviews(@PathVariable long id) {
        return catalog.getReviews(id);
    }

    @PutMapping("/products/{id}")
    public ProductResponse updateProduct(@PathVariable long id, @RequestBody ProductUpdateRequest request) {
        return catalog.updateProduct(id, request);
    }

    @PostMapping("/products/{id}/reviews")
    public ReviewResponse addReview(@PathVariable long id, @RequestBody ReviewCreateRequest request) {
        return catalog.addReview(id, request);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return catalog.stats();
    }
}
