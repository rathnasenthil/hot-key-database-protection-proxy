package com.hotkey.proxy.cache;

import java.time.Duration;

public record CachePolicy(Duration ttl, Duration staleWindow, Duration hotTtl) {
}
