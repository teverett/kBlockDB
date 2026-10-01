package com.kblockdb.client;

/**
 * On-disk world statistics returned by {@link KBlockDBClient#stats()}.
 */
public record Stats(long totalChunks, long totalBytes, long totalBlocks) {
}
