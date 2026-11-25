package com.hotkey.proxy.hotkey;

public record HotKeyInfo(
        String key,
        double requestsPerSecond,
        boolean promoted,
        int windowSeconds
) {
}
