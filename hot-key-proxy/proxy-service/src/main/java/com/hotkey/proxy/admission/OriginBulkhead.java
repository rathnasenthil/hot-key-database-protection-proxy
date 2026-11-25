package com.hotkey.proxy.admission;

import com.hotkey.proxy.config.ProxyProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Limits concurrent origin calls and keeps a short failure window so the proxy
 * can prefer a stale value when the origin is unhealthy.
 */
@Component
public class OriginBulkhead {

    private final Semaphore permits;
    private final Semaphore queueSlots;
    private final Duration queueWait;
    private final int minSamples;
    private final double failureRate;
    private final AtomicInteger inUse = new AtomicInteger();
    private final AtomicIntegerArray outcomes;
    private final AtomicInteger cursor = new AtomicInteger();

    @Autowired
    public OriginBulkhead(ProxyProperties properties) {
        ProxyProperties.Admission admission = properties.getAdmission();
        this.permits = new Semaphore(Math.max(1, admission.getMaxOriginConcurrency()), true);
        this.queueSlots = new Semaphore(Math.max(0, admission.getQueueLimit()), true);
        this.queueWait = admission.getQueueWait();
        this.minSamples = admission.getUnhealthyMinSamples();
        this.failureRate = admission.getUnhealthyFailureRate();
        this.outcomes = new AtomicIntegerArray(32);
        for (int i = 0; i < outcomes.length(); i++) {
            outcomes.set(i, -1);
        }
    }

    public OriginBulkhead(int concurrency, int queueLimit, Duration queueWait, int minSamples, double failureRate) {
        this.permits = new Semaphore(Math.max(1, concurrency), true);
        this.queueSlots = new Semaphore(Math.max(0, queueLimit), true);
        this.queueWait = queueWait;
        this.minSamples = minSamples;
        this.failureRate = failureRate;
        this.outcomes = new AtomicIntegerArray(32);
        for (int i = 0; i < outcomes.length(); i++) {
            outcomes.set(i, -1);
        }
    }

    public boolean tryEnter() {
        if (permits.tryAcquire()) {
            inUse.incrementAndGet();
            return true;
        }
        if (!queueSlots.tryAcquire()) {
            return false;
        }
        try {
            boolean acquired = permits.tryAcquire(Math.max(1, queueWait.toMillis()), TimeUnit.MILLISECONDS);
            if (!acquired) {
                return false;
            }
            inUse.incrementAndGet();
            return true;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            queueSlots.release();
        }
    }

    public void leave() {
        inUse.decrementAndGet();
        permits.release();
    }

    public void record(boolean success) {
        int index = Math.floorMod(cursor.getAndIncrement(), outcomes.length());
        outcomes.set(index, success ? 0 : 1);
    }

    public boolean preferStale() {
        int samples = 0;
        int failures = 0;
        for (int i = 0; i < outcomes.length(); i++) {
            int value = outcomes.get(i);
            if (value < 0) {
                continue;
            }
            samples++;
            if (value == 1) {
                failures++;
            }
        }
        return samples >= minSamples && (failures / (double) samples) >= failureRate;
    }

    public int inUse() {
        return Math.max(0, inUse.get());
    }
}
