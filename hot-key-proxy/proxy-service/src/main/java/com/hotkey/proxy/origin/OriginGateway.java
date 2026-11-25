package com.hotkey.proxy.origin;

import org.springframework.http.HttpMethod;

public interface OriginGateway {

    OriginResult execute(HttpMethod method, String path, byte[] body, String contentType);

    default boolean preferStale() {
        return false;
    }
}
