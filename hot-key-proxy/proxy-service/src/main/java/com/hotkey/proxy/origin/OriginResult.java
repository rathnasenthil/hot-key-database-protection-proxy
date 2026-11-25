package com.hotkey.proxy.origin;

public record OriginResult(Kind kind, int status, String contentType, byte[] body, String reason) {

    public enum Kind {
        OK,
        REJECTED,
        FAILED,
        UNKNOWN
    }

    public OriginResult {
        body = body == null ? new byte[0] : body;
    }

    public static OriginResult ok(int status, String contentType, byte[] body) {
        return new OriginResult(Kind.OK, status, contentType, body, null);
    }

    public static OriginResult rejected() {
        return new OriginResult(Kind.REJECTED, 429, "application/json", new byte[0], null);
    }

    public static OriginResult failed() {
        return new OriginResult(Kind.FAILED, 502, "application/json", new byte[0], null);
    }

    /**
     * The write may or may not have reached the origin. {@code reason} is {@code timeout} or {@code connection}.
     */
    public static OriginResult unknown(String reason) {
        String normalized = "timeout".equals(reason) ? "timeout" : "connection";
        return new OriginResult(Kind.UNKNOWN, 504, "application/json", new byte[0], normalized);
    }
}
