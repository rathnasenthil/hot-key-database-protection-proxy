package com.hotkey.origin.catalog;

import com.hotkey.origin.OriginProperties;
import com.hotkey.origin.api.ProductResponse;
import com.hotkey.origin.api.ProductUpdateRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CatalogServiceWriteStatusTest {

    @Mock
    private CatalogRepository repository;

    private CatalogService service;

    @BeforeEach
    void setUp() {
        service = new CatalogService(repository, new OriginProperties());
    }

    @Test
    void missingProductBeforeUpdateIs404AndDoesNotUpdate() {
        when(repository.findProduct(8L)).thenReturn(Optional.empty());

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                service.updateProduct(8L, update(new BigDecimal("2.00"))));

        assertEquals(404, exception.getStatusCode().value());
        verify(repository, never()).updateProduct(anyLong(), any(), any());
    }

    @Test
    void negativePriceIs400AndDoesNotUpdate() {
        when(repository.findProduct(8L)).thenReturn(Optional.of(product(new BigDecimal("1.00"))));

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                service.updateProduct(8L, update(new BigDecimal("-1"))));

        assertEquals(400, exception.getStatusCode().value());
        verify(repository, never()).updateProduct(anyLong(), any(), any());
    }

    @Test
    void readFailureAfterUpdateIs500() {
        ProductResponse current = product(new BigDecimal("1.00"));
        when(repository.findProduct(8L)).thenReturn(Optional.of(current), Optional.empty());
        when(repository.updateProduct(eq(8L), any(), eq(current))).thenReturn(1);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                service.updateProduct(8L, update(new BigDecimal("2.00"))));

        assertEquals(500, exception.getStatusCode().value());
        verify(repository).updateProduct(eq(8L), any(), eq(current));
    }

    private static ProductResponse product(BigDecimal price) {
        return new ProductResponse(8L, "Product 8", "description", price, "books", Instant.parse("2026-01-01T00:00:00Z"));
    }

    private static ProductUpdateRequest update(BigDecimal price) {
        return new ProductUpdateRequest("Product 8", "description", price, "books");
    }
}
