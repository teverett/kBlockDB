package com.kblockdb.client;

/**
 * Missing or invalid credentials -- either {@link KBlockDBClient#connect}
 * itself, {@link KBlockDBClient#reauthenticate}, or (if the connection
 * never sent a successful {@code Hello}) any other request.
 */
public final class UnauthorizedException extends KBlockDBException {

    private static final long serialVersionUID = 1L;

    UnauthorizedException(String message) {
        super(message);
    }
}
