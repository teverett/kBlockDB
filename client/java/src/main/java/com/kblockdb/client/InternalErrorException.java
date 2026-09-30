package com.kblockdb.client;

/**
 * The server reported an internal error while handling an otherwise
 * well-formed, authorized request (e.g. a disk I/O failure).
 */
public final class InternalErrorException extends KBlockDBException {

    private static final long serialVersionUID = 1L;

    InternalErrorException(String message) {
        super(message);
    }
}
