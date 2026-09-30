package com.kblockdb.client;

/**
 * The peer sent bytes that don't parse as this protocol expects -- an
 * unknown status or value-type tag, a truncated frame, a response kind
 * that doesn't fit the request that produced it, or a frame larger than
 * the protocol's own limit. Always indicates a bug or a version mismatch
 * between client and server, never a well-formed application-level error
 * (those are the other {@link KBlockDBException} subtypes).
 */
public final class ProtocolException extends KBlockDBException {

    private static final long serialVersionUID = 1L;

    ProtocolException(String message) {
        super(message);
    }
}
