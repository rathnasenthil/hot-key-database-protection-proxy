package com.hotkey.proxy.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;

@Component
public class LocalHotCache {

    private final Cache<String, CacheEntry> entries = Caffeine.newBuilder()
            .maximumSize(20_000)
            .build();

    public CacheEntry getIfFresh(String key) {
        CacheEntry entry = entries.getIfPresent(key);
        if (entry == null) {
            return null;
        }
        if (entry.fresh(System.currentTimeMillis())) {
            return entry;
        }
        entries.invalidate(key);
        return null;
    }

    public void put(String key, CacheEntry entry) {
        if (entry != null) {
            entries.put(key, entry);
        }
    }

    public void invalidate(String key) {
        entries.invalidate(key);
    }
}
