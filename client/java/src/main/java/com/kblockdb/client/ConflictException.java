package com.kblockdb.client;

/**
 * The request conflicts with the world's current state -- today, only
 * {@link KBlockDBClient#addColumn(String, ValueType)} against a key that
 * already has a column.
 */
public final class ConflictException extends KBlockDBException {

    private static final long serialVersionUID = 1L;

    ConflictException(String message) {
        super(message);
    }
}
