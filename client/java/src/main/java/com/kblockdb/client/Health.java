package com.kblockdb.client;

/**
 * The server's current shape and clock, returned by {@link KBlockDBClient#health()}.
 *
 * @param axes world axis count
 * @param worldDim cells per axis
 * @param timestamp seconds since the Unix epoch according to the server
 */
public record Health(int axes, long worldDim, long timestamp) {
}
