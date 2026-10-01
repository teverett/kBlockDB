package com.kblockdb.client;

import java.util.Objects;

/** One selected key/value and its metadata within a query row. */
public record QueryValue(String key, Value value, CellMeta meta) {
    public QueryValue {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(meta, "meta");
    }
}
