package com.hotkey.proxy.hash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Consistent hash ring with virtual nodes. The same key always maps to the same
 * physical node until the membership set changes.
 */
public final class ConsistentHashRing<T> {

    private final TreeMap<Long, T> ring = new TreeMap<>();

    public ConsistentHashRing(Collection<T> nodes, int virtualNodes, Function<T, String> identity) {
        if (virtualNodes < 1) {
            throw new IllegalArgumentException("virtualNodes must be >= 1");
        }
        for (T node : nodes) {
            String id = identity.apply(node);
            for (int vnode = 0; vnode < virtualNodes; vnode++) {
                ring.put(hash(id + "#" + vnode), node);
            }
        }
    }

    public boolean isEmpty() {
        return ring.isEmpty();
    }

    public T owner(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("hash ring has no nodes");
        }
        long hashed = hash(key);
        Map.Entry<Long, T> entry = ring.ceilingEntry(hashed);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    public List<T> owners(String key, int limit) {
        if (ring.isEmpty() || limit <= 0) {
            return List.of();
        }
        long hashed = hash(key);
        Map.Entry<Long, T> start = ring.ceilingEntry(hashed);
        if (start == null) {
            start = ring.firstEntry();
        }
        List<T> ordered = new ArrayList<>();
        for (T node : ring.tailMap(start.getKey(), true).values()) {
            if (!ordered.contains(node)) {
                ordered.add(node);
            }
            if (ordered.size() >= limit) {
                return List.copyOf(ordered);
            }
        }
        for (T node : ring.values()) {
            if (!ordered.contains(node)) {
                ordered.add(node);
            }
            if (ordered.size() >= limit) {
                break;
            }
        }
        return List.copyOf(ordered);
    }

    static long hash(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("MD5").digest(value.getBytes(StandardCharsets.UTF_8));
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (bytes[i] & 0xffL);
            }
            return hash;
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
