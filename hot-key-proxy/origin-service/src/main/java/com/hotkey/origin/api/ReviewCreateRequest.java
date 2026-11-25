package com.hotkey.origin.api;

public record ReviewCreateRequest(
        Long userId,
        Integer rating,
        String comment
) {
}
