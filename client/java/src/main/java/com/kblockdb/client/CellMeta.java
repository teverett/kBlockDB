package com.kblockdb.client;

/**
 * A cell/key pair's bookkeeping, alongside its value: when it was first
 * set, when it was last changed, and how many times it's been set since
 * (see {@code kblockdblib::CellMeta}, which this mirrors field-for-field).
 *
 * <p>{@code version} is {@code 0} for a value that's never been
 * overwritten, incremented on every later {@code set}. Removing a value
 * and setting it again later starts a fresh {@code 0}/{@code createdAtMs}
 * -- it's a new history, not a continuation of the old one.
 *
 * <p>{@code createdAtMs}/{@code modifiedAtMs} are milliseconds since the
 * Unix epoch. kBlockDB's wire format carries these as unsigned 64-bit
 * integers; Java has no unsigned 64-bit type, but the values in practice
 * (timestamps, an overwrite count) fit comfortably, and always positively,
 * in a {@code long}.
 */
public record CellMeta(long createdAtMs, long modifiedAtMs, long version) {
}
