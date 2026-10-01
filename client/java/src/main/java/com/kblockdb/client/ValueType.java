package com.kblockdb.client;

/**
 * The type a schema column is fixed to -- the same four kinds
 * {@link Value} has, but named on their own, without a value attached.
 * Mirrors {@code kblockdblib::ValueType}.
 *
 * <p>{@link #tag()} is the one-byte wire tag a {@link Value} of this type
 * carries, so a column declaration and an encoded value always agree
 * about what a type is.
 */
public enum ValueType {

    /** A UTF-8 string column -- {@link Value.Str}. */
    STR(0, "str"),

    /** A 64-bit floating-point column -- {@link Value.F64}. */
    F64(1, "f64"),

    /** A 64-bit signed integer column -- {@link Value.I64}. */
    I64(2, "i64"),

    /** A boolean column -- {@link Value.Bool}. */
    BOOL(3, "bool");

    private final int tag;
    private final String wireName;

    ValueType(int tag, String wireName) {
        this.tag = tag;
        this.wireName = wireName;
    }

    /** The one-byte type tag this type uses on the wire. */
    public int tag() {
        return tag;
    }

    /**
     * The short, stable name the server uses for this type in
     * {@code schema.txt} and in its REST API ({@code "str"},
     * {@code "f64"}, {@code "i64"}, {@code "bool"}).
     */
    public String wireName() {
        return wireName;
    }

    /**
     * The type of a given {@link Value} -- what column that value would
     * need in order to be stored.
     */
    public static ValueType of(Value value) {
        if (value instanceof Value.Str) {
            return STR;
        }
        if (value instanceof Value.F64) {
            return F64;
        }
        if (value instanceof Value.I64) {
            return I64;
        }
        if (value instanceof Value.Bool) {
            return BOOL;
        }
        throw new IllegalArgumentException("unknown Value subtype: " + value.getClass());
    }

    /** The inverse of {@link #tag()}, or {@code null} for an unknown tag. */
    static ValueType fromTag(int tag) {
        for (ValueType type : values()) {
            if (type.tag == tag) {
                return type;
            }
        }
        return null;
    }
}
