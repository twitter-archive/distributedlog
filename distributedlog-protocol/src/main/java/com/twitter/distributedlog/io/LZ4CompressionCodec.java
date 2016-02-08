package com.twitter.distributedlog.io;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * All functions are thread safe.
 */
public class LZ4CompressionCodec implements CompressionCodec {

    // Used for compression
    private final LZ4Compressor compressor;
    // Used to decompress when the size of the output is known
    private final LZ4FastDecompressor fastDecompressor;
    // Used to decompress when the size of the output is not known
    private final LZ4SafeDecompressor safeDecompressor;

    public LZ4CompressionCodec() {
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
        this.fastDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
        this.safeDecompressor = LZ4Factory.fastestInstance().safeDecompressor();
    }

    @Override
    public byte[] compress(byte[] data, int length, OpStatsLogger compressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(length >= 0);
        Preconditions.checkNotNull(compressionStat);

        Stopwatch watch = Stopwatch.createStarted();
        byte[] compressed = compressor.compress(data, 0, length);
        compressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
        return compressed;
    }

    @Override
    public byte[] decompress(byte[] data, int length, OpStatsLogger decompressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(length >= 0);
        Preconditions.checkNotNull(decompressionStat);

        Stopwatch watch = Stopwatch.createStarted();
        // Assume that we have a compression ratio of 1/3.
        int outLength = length * 3;
        while (true) {
            try {
                byte[] decompressed = safeDecompressor.decompress(data, 0, length, outLength);
                decompressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
                return decompressed;
            } catch (LZ4Exception e) {
                outLength *= 2;
            }
        }
    }

    @Override
    // length parameter is ignored here because of the way the fastDecompressor works.
    public byte[] decompress(byte[] data, int length, int decompressedSize,
                             OpStatsLogger decompressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(decompressedSize >= 0);
        Preconditions.checkNotNull(decompressionStat);

        Stopwatch watch = Stopwatch.createStarted();
        byte[] decompressed = fastDecompressor.decompress(data, decompressedSize);
        decompressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
        return decompressed;
    }
}
