package com.hotkey.proxy.cache;

public record CacheEntry(
        int status,
        String contentType,
        byte[] body,
        long freshUntil,
        long staleUntil,
        boolean negative
) {
    public boolean fresh(long now) {
        return now < freshUntil;
    }

    public boolean stale(long now) {
        return now >= freshUntil && now < staleUntil;
    }
}
