package com.kblockdb.client;

import java.util.Objects;

/**
 * The server's identity, world shape, and clock, returned by
 * {@link KBlockDBClient#health()}.
 *
 * @param hostname which instance answered -- its OS hostname, or whatever
 *                 the server's {@code hostname} config key overrides it
 *                 to. Never empty: a server that can't determine its own
 *                 hostname reports {@code "unknown"}.
 * @param axes world axis count
 * @param worldDim cells per axis
 * @param chunkDim cells per axis in one chunk file -- the world's on-disk
 *                 granularity, fixed when the world was created
 * @param timestamp seconds since the Unix epoch according to the server
 */
public record Health(String hostname, int axes, long worldDim, long chunkDim, long timestamp) {

    public Health {
        Objects.requireNonNull(hostname, "hostname");
    }
}
