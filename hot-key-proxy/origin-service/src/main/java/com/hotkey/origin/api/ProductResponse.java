package com.hotkey.origin.api;

import java.math.BigDecimal;
import java.time.Instant;

public record ProductResponse(
        long id,
        String name,
        String description,
        BigDecimal price,
        String category,
        Instant createdAt
) {
}
