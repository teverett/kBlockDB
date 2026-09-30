package com.kblockdb.client;

/**
 * A read-only account attempted a {@link KBlockDBClient#set} or
 * {@link KBlockDBClient#remove}.
 */
public final class ForbiddenException extends KBlockDBException {

    private static final long serialVersionUID = 1L;

    ForbiddenException(String message) {
        super(message);
    }
}
