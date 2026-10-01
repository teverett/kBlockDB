package com.kblockdb.client;

import java.util.Objects;

/**
 * One column in a world's schema: a key, and the {@link ValueType} fixed
 * for it when the column was created. Returned by
 * {@link KBlockDBClient#columns()}.
 */
public record Column(String key, ValueType valueType) {

    public Column {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valueType, "valueType");
    }
}
