package com.hotkey.proxy.coalesce;

public record CoalesceResult<T>(T value, boolean leader, boolean shared) {
}
