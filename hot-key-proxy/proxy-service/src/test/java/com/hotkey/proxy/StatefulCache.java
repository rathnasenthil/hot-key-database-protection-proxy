package com.hotkey.proxy;

import com.hotkey.proxy.cache.CacheEntry;
import com.hotkey.proxy.cache.CacheLookup;
import com.hotkey.proxy.cache.CachePolicy;
import com.hotkey.proxy.cache.ResponseCache;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stand-in for the generation contract. A missing token does not match a store.
 */
final class StatefulCache implements ResponseCache {

    private static final SecureRandom TOKENS = new SecureRandom();

    private final ConcurrentHashMap<String, CacheEntry> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> generations = new ConcurrentHashMap<>();
    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private boolean dropPinnedGeneration;
    private boolean refusePins;

    void preload(String key, CacheEntry entry) {
        entries.put(key, entry);
    }

    void dropPinnedGenerationBeforeStore() {
        dropPinnedGeneration = true;
    }

    void refusePins() {
        refusePins = true;
    }

    void evict(String key) {
        generations.remove(key);
        entries.remove(key);
        events.add("evict:" + key);
    }

    List<String> events() {
        return List.copyOf(events);
    }

    String generation(String key) {
        return generations.get(key);
    }

    Long freshUntil(String key) {
        CacheEntry entry = entries.get(key);
        return entry == null ? null : entry.freshUntil();
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public CacheLookup lookup(String key) {
        CacheEntry entry = entries.get(key);
        if (entry == null) {
            return CacheLookup.miss();
        }
        long now = System.currentTimeMillis();
        if (entry.fresh(now)) {
            return CacheLookup.fresh(entry);
        }
        if (entry.stale(now)) {
            return CacheLookup.stale(entry);
        }
        return CacheLookup.miss();
    }

    @Override
    public CacheEntry store(String key, int status, String contentType, byte[] body, CachePolicy policy, boolean hot) {
        events.add("store:" + key);
        long now = System.currentTimeMillis();
        CacheEntry entry = new CacheEntry(status, contentType, body, now + 60_000, now + 90_000, status == 404);
        entries.put(key, entry);
        return entry;
    }

    @Override
    public Optional<String> pinGeneration(String key) {
        if (refusePins) {
            events.add("pin-unavailable:" + key);
            return Optional.empty();
        }
        String pinned = generations.compute(key, (ignored, existing) -> existing == null ? newToken() : existing);
        events.add("pin:" + key + ":" + pinned);
        if (dropPinnedGeneration) {
            generations.remove(key);
            entries.remove(key);
            dropPinnedGeneration = false;
            events.add("drop-generation:" + key);
        }
        return Optional.of(pinned);
    }

    @Override
    public CacheEntry storeIfGeneration(String key, String generation, int status, String contentType, byte[] body, CachePolicy policy, boolean hot) {
        String current = generations.get(key);
        if (current == null || generation == null || !current.equals(generation)) {
            events.add("reject-store:" + key);
            return null;
        }
        return store(key, status, contentType, body, policy, hot);
    }

    @Override
    public void invalidate(String key) {
        events.add("invalidate:" + key);
        generations.put(key, newToken());
        entries.remove(key);
    }

    private static String newToken() {
        byte[] bytes = new byte[16];
        TOKENS.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
