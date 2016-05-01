package com.twitter.distributedlog.io;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import org.apache.bookkeeper.stats.OpStatsLogger;

public class IdentityCompressionCodec implements CompressionCodec {
    @Override
    public byte[] compress(byte[] data, int offset, int length, OpStatsLogger compressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(length >= 0);
        return Arrays.copyOfRange(data, offset, offset + length);
    }

    @Override
    public byte[] decompress(byte[] data, int offset, int length, OpStatsLogger decompressionStat) {
        Preconditions.checkNotNull(data);
        return Arrays.copyOfRange(data, offset, offset + length);
    }

    @Override
    // Decompressed size is the same as the length of the data because this is an
    // Identity compressor
    public byte[] decompress(byte[] data, int offset, int length,
                             int decompressedSize, OpStatsLogger decompressionStat) {
        return decompress(data, offset, length, decompressionStat);
    }
}
