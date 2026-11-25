package com.hotkey.proxy.cache;

import java.util.Optional;

public interface ResponseCache {

    boolean available();

    CacheLookup lookup(String key);

    CacheEntry store(String key, int status, String contentType, byte[] body, CachePolicy policy, boolean hot);

    /**
     * Ensures the key has a generation token and returns it.
     * Empty means the token is unavailable. Callers must not store in that case.
     * An empty result is not a generation value and must not be compared with a later absence.
     */
    Optional<String> pinGeneration(String key);

    /**
     * Stores only when {@code generation} is still the key's token.
     * A missing token does not match. Returns null when the store is refused.
     */
    CacheEntry storeIfGeneration(String key, String generation, int status, String contentType, byte[] body, CachePolicy policy, boolean hot);

    void invalidate(String key);
}
