package com.kblockdb.client;

import java.util.List;
import java.util.Objects;

/**
 * Result of {@link KBlockDBClient#query(String)}: rows for {@code SELECT},
 * or an affected-cell count for {@code SET}, {@code UPDATE}, and
 * {@code DELETE}.
 */
public sealed interface QueryResult {

    /** A {@code SELECT} result. */
    record Rows(List<QueryRow> rows) implements QueryResult {
        public Rows {
            rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        }

        public int totalRows() {
            return rows.size();
        }
    }

    /** A mutating query result. */
    record Affected(long affectedCells) implements QueryResult {
    }
}
