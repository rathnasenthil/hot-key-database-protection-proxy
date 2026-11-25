package com.hotkey.origin;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "origin")
public class OriginProperties {

    /**
     * Optional per-read delay used to model a heavier origin query.
     * Default is zero; PostgreSQL itself is the latency source.
     */
    private long extraDelayMs;

    public long getExtraDelayMs() {
        return extraDelayMs;
    }

    public void setExtraDelayMs(long extraDelayMs) {
        this.extraDelayMs = extraDelayMs;
    }
}
