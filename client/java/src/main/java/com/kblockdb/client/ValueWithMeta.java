package com.kblockdb.client;

/**
 * A cell's value together with its {@link CellMeta} -- returned by
 * {@link KBlockDBClient#getWithMeta}, which reads both from the same
 * server response so they're always consistent with each other (see that
 * method's doc comment).
 */
public record ValueWithMeta(Value value, CellMeta meta) {
}
