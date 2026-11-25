package com.hotkey.proxy.coalesce;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

@Component
public class CoalescingEngine {

    private final ConcurrentHashMap<String, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    public <T> CoalesceResult<T> execute(String key, Duration maxWait, Supplier<T> leader, Supplier<T> fallback) {
        CompletableFuture<Object> created = new CompletableFuture<>();
        CompletableFuture<Object> existing = inflight.putIfAbsent(key, created);
        if (existing == null) {
            try {
                T value = leader.get();
                created.complete(value);
                return new CoalesceResult<>(value, true, false);
            } catch (Throwable failure) {
                created.completeExceptionally(failure);
                if (failure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (failure instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException(failure);
            } finally {
                inflight.remove(key, created);
            }
        }
        joinedInFlight(key);
        try {
            long waitMs = Math.max(1, maxWait.toMillis());
            @SuppressWarnings("unchecked")
            T value = (T) existing.get(waitMs, TimeUnit.MILLISECONDS);
            return new CoalesceResult<>(value, false, true);
        } catch (TimeoutException | ExecutionException exception) {
            return new CoalesceResult<>(fallback.get(), false, false);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return new CoalesceResult<>(fallback.get(), false, false);
        }
    }

    /**
     * Called when a request attaches to a load that is already running.
     */
    protected void joinedInFlight(String key) {
    }
}
