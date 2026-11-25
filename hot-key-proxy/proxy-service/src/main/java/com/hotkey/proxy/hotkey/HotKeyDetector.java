package com.hotkey.proxy.hotkey;

import com.hotkey.proxy.config.RuntimeSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

@Component
public class HotKeyDetector {

    private static final int BUCKETS = 10;

    private final long windowMs;
    private final long bucketMs;
    private final DoubleSupplier threshold;
    private final DoubleSupplier demoteFraction;
    private final LongSupplier clock;
    private final ConcurrentHashMap<String, KeyCounter> counters = new ConcurrentHashMap<>();
    private final Set<String> promoted = ConcurrentHashMap.newKeySet();

    @Autowired
    public HotKeyDetector(RuntimeSettings settings) {
        this(settings.hotKeyWindow(), settings::hotKeyThreshold, settings::demoteFraction, System::currentTimeMillis);
    }

    public HotKeyDetector(Duration window, DoubleSupplier threshold, DoubleSupplier demoteFraction, LongSupplier clock) {
        this.windowMs = Math.max(BUCKETS, window.toMillis());
        this.bucketMs = Math.max(1, windowMs / BUCKETS);
        this.threshold = threshold;
        this.demoteFraction = demoteFraction;
        this.clock = clock;
    }

    public boolean record(String key) {
        long now = clock.getAsLong();
        KeyCounter counter = counters.computeIfAbsent(key, ignored -> new KeyCounter(BUCKETS));
        int count = counter.record(now, bucketMs);
        double rps = count * 1000.0 / windowMs;
        double limit = threshold.getAsDouble();
        if (rps >= limit) {
            promoted.add(key);
        } else if (rps < limit * demoteFraction.getAsDouble()) {
            promoted.remove(key);
        }
        return promoted.contains(key);
    }

    public boolean isHot(String key) {
        return promoted.contains(key);
    }

    public int promotedCount() {
        return promoted.size();
    }

    public List<HotKeyInfo> top(int limit) {
        long now = clock.getAsLong();
        int windowSeconds = (int) Math.max(1, windowMs / 1000);
        return counters.entrySet().stream()
                .map(entry -> {
                    int count = entry.getValue().sum(now, bucketMs);
                    double rps = count * 1000.0 / windowMs;
                    return new HotKeyInfo(entry.getKey(), round(rps), promoted.contains(entry.getKey()), windowSeconds);
                })
                .sorted(Comparator.comparingDouble(HotKeyInfo::requestsPerSecond).reversed())
                .limit(limit)
                .toList();
    }

    @Scheduled(fixedDelay = 5000)
    public void evictCold() {
        long now = clock.getAsLong();
        counters.entrySet().removeIf(entry -> entry.getValue().sum(now, bucketMs) == 0);
        promoted.removeIf(key -> !counters.containsKey(key));
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    static final class KeyCounter {
        private final int[] counts;
        private long currentBucketId = Long.MIN_VALUE;

        KeyCounter(int buckets) {
            this.counts = new int[buckets];
        }

        synchronized int record(long now, long bucketMs) {
            rotate(now / bucketMs);
            counts[index(currentBucketId)]++;
            return sumCounts();
        }

        synchronized int sum(long now, long bucketMs) {
            rotate(now / bucketMs);
            return sumCounts();
        }

        private void rotate(long bucketId) {
            if (currentBucketId == Long.MIN_VALUE) {
                currentBucketId = bucketId;
                return;
            }
            long age = bucketId - currentBucketId;
            if (age <= 0) {
                return;
            }
            if (age >= counts.length) {
                Arrays.fill(counts, 0);
            } else {
                for (long bucket = currentBucketId + 1; bucket <= bucketId; bucket++) {
                    counts[index(bucket)] = 0;
                }
            }
            currentBucketId = bucketId;
        }

        private int sumCounts() {
            int sum = 0;
            for (int count : counts) {
                sum += count;
            }
            return sum;
        }

        private int index(long bucketId) {
            return (int) Math.floorMod(bucketId, counts.length);
        }
    }
}
