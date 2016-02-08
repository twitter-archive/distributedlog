package com.twitter.distributedlog.io;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import org.apache.bookkeeper.stats.OpStatsLogger;

public class IdentityCompressionCodec implements CompressionCodec {
    @Override
    public byte[] compress(byte[] data, int length, OpStatsLogger compressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(length >= 0);
        return Arrays.copyOf(data, length);
    }

    @Override
    public byte[] decompress(byte[] data, int length, OpStatsLogger decompressionStat) {
        Preconditions.checkNotNull(data);
        return Arrays.copyOf(data, length);
    }

    @Override
    // Decompressed size is the same as the length of the data because this is an
    // Identity compressor
    public byte[] decompress(byte[] data, int length,
                             int decompressedSize, OpStatsLogger decompressionStat) {
        return decompress(data, length, decompressionStat);
    }
}
