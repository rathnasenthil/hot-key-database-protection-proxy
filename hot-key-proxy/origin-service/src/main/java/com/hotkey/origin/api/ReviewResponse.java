package com.hotkey.origin.api;

import java.time.Instant;

public record ReviewResponse(
        long id,
        long productId,
        long userId,
        int rating,
        String comment,
        Instant createdAt
) {
}
