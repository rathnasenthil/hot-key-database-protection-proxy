package com.hotkey.origin.catalog;

import com.hotkey.origin.OriginProperties;
import com.hotkey.origin.api.ProductResponse;
import com.hotkey.origin.api.ProductUpdateRequest;
import com.hotkey.origin.api.ReviewCreateRequest;
import com.hotkey.origin.api.ReviewResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@Service
public class CatalogService {

    private final CatalogRepository repository;
    private final OriginProperties properties;

    public CatalogService(CatalogRepository repository, OriginProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    public ProductResponse getProduct(long id) {
        delay();
        return repository.findProduct(id)
                .orElseThrow(() -> notFound("product", id));
    }

    public List<ReviewResponse> getReviews(long id) {
        delay();
        if (!repository.productExists(id)) {
            throw notFound("product", id);
        }
        return repository.findReviews(id);
    }

    public ProductResponse updateProduct(long id, ProductUpdateRequest request) {
        ProductResponse current = repository.findProduct(id)
                .orElseThrow(() -> notFound("product", id));
        if (request.price() != null && request.price().signum() < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "price must be >= 0");
        }
        repository.updateProduct(id, request, current);
        return repository.findProduct(id).orElseThrow(() -> new ResponseStatusException(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "product " + id + " was updated but could not be read back"
        ));
    }

    public ReviewResponse addReview(long productId, ReviewCreateRequest request) {
        if (!repository.productExists(productId)) {
            throw notFound("product", productId);
        }
        if (request.userId() == null || request.rating() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "userId and rating are required");
        }
        if (request.rating() < 1 || request.rating() > 5) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "rating must be between 1 and 5");
        }
        return repository.insertReview(productId, request);
    }

    public Map<String, Object> stats() {
        return Map.of("products", repository.productCount());
    }

    private void delay() {
        long delayMs = properties.getExtraDelayMs();
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "interrupted");
        }
    }

    private static ResponseStatusException notFound(String type, long id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, type + " " + id + " not found");
    }
}
