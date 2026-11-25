package com.hotkey.proxy.cache;

public record StoredValue(
        int status,
        String contentType,
        String bodyBase64,
        long freshUntil,
        long staleUntil,
        boolean negative
) {
}
