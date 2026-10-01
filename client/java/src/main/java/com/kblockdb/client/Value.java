package com.kblockdb.client;

import java.util.Objects;

/**
 * A cell's value -- kBlockDB stores exactly one of these four primitive
 * kinds per (coordinate, key) pair. Mirrors {@code kblockdblib::Value} on
 * the server side and its wire-protocol encoding in
 * {@code kblockdbserver/src/wire.rs}.
 */
public sealed interface Value {

    /** A UTF-8 string value. */
    record Str(String value) implements Value {
        public Str {
            Objects.requireNonNull(value, "value");
        }
    }

    /** A 64-bit signed integer value. */
    record I64(long value) implements Value {
    }

    /** A 64-bit floating-point value. */
    record F64(double value) implements Value {
    }

    /** A boolean value. */
    record Bool(boolean value) implements Value {
    }
}
