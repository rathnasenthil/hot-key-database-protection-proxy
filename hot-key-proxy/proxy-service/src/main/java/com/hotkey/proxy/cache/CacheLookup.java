package com.hotkey.proxy.cache;

public record CacheLookup(Status status, CacheEntry entry) {

    public enum Status {
        FRESH,
        STALE,
        MISS,
        UNAVAILABLE
    }

    public static CacheLookup fresh(CacheEntry entry) {
        return new CacheLookup(Status.FRESH, entry);
    }

    public static CacheLookup stale(CacheEntry entry) {
        return new CacheLookup(Status.STALE, entry);
    }

    public static CacheLookup miss() {
        return new CacheLookup(Status.MISS, null);
    }

    public static CacheLookup unavailable() {
        return new CacheLookup(Status.UNAVAILABLE, null);
    }
}
