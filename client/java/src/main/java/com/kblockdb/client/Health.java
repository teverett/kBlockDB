package com.kblockdb.client;

import java.util.Objects;

/**
 * The server's identity, how many databases it manages, and its clock,
 * returned by {@link KBlockDBClient#health()}.
 *
 * @param hostname which instance answered -- its OS hostname, or whatever
 *                 the server's {@code hostname} config key overrides it
 *                 to. Never empty: a server that can't determine its own
 *                 hostname reports {@code "unknown"}.
 * @param databaseCount how many databases this server currently manages --
 *                      a live count, not a cached one
 * @param timestamp seconds since the Unix epoch according to the server
 */
public record Health(String hostname, long databaseCount, long timestamp) {

    public Health {
        Objects.requireNonNull(hostname, "hostname");
    }
}
