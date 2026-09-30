package com.kblockdb.client;

import java.io.IOException;

/**
 * Base type for every well-formed protocol-level error kBlockDB's binary
 * protocol can report back (as opposed to a plain connection/framing
 * failure, which surfaces as a plain {@link IOException} or one of its
 * standard subtypes, e.g. {@link java.io.EOFException}).
 */
public class KBlockDBException extends IOException {

    private static final long serialVersionUID = 1L;

    KBlockDBException(String message) {
        super(message);
    }
}
