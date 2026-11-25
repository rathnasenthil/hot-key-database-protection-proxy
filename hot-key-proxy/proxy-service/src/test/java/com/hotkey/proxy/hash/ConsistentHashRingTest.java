package com.hotkey.proxy.hash;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashRingTest {

    @Test
    void sameKeyAlwaysMapsToTheSameNode() {
        ConsistentHashRing<String> ring = ring(List.of("a", "b", "c"));
        String first = ring.owner("product:42");
        for (int i = 0; i < 100; i++) {
            assertEquals(first, ring.owner("product:42"));
        }
    }

    @Test
    void virtualNodesSpreadKeysAcrossTheRing() {
        ConsistentHashRing<String> ring = ring(List.of("a", "b", "c"));
        Map<String, Long> counts = IntStream.rangeClosed(1, 12_000)
                .mapToObj(id -> ring.owner("product:" + id))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        assertEquals(3, counts.size());
        counts.values().forEach(count -> {
            assertTrue(count > 2_000, "count=" + count);
            assertTrue(count < 6_000, "count=" + count);
        });
    }

    @Test
    void ownersWalksToDistinctFailoverNodes() {
        ConsistentHashRing<String> ring = ring(List.of("a", "b", "c"));
        List<String> owners = ring.owners("product:7", 3);
        assertEquals(3, owners.stream().distinct().count());
        assertEquals(owners.get(0), ring.owner("product:7"));
    }

    private static ConsistentHashRing<String> ring(List<String> nodes) {
        return new ConsistentHashRing<>(nodes, 150, Function.identity());
    }
}
