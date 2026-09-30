package com.kblockdb.client;

/**
 * The server rejected the request itself -- an invalid coordinate, the
 * wrong axis count for this world, a value type that doesn't match a
 * key's existing type, and the like.
 */
public final class BadRequestException extends KBlockDBException {

    private static final long serialVersionUID = 1L;

    BadRequestException(String message) {
        super(message);
    }
}
