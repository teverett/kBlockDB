package com.kblockdb.client;

import java.util.List;
import java.util.Objects;

/** One coordinate and its selected values in a {@code SELECT} result. */
public record QueryRow(List<Integer> coord, List<QueryValue> values) {
    public QueryRow {
        coord = List.copyOf(Objects.requireNonNull(coord, "coord"));
        values = List.copyOf(Objects.requireNonNull(values, "values"));
    }
}
