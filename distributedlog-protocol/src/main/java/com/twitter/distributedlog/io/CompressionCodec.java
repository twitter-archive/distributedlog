package com.twitter.distributedlog.io;

import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * Common interface for compression/decompression operations using different
 * compression codecs.
 */
public interface CompressionCodec {
    /**
     * Enum specifying the currently supported compression types.
     */
    public static enum Type {
        NONE, LZ4, UNKNOWN
    }

    /**
     * Return the compressed data as a byte array.
     * @param data
     *          The data to be compressed
     * @param length
     *          The number of bytes of data to compress
     * @param compressionStat
     *          The stat to use for timing the compression operation
     * @return
     *          The compressed data
     *          The returned byte array is sized to the length of the compressed data
     */
    byte[] compress(byte[] data, int length, OpStatsLogger compressionStat);

    /**
     * Return the decompressed data as a byte array.
     * @param data
     *          The data to be decompressed
     * @param length
     *          The number of bytes of data to decompress
     * @param decompressionStat
     *          The stat to use for timing the decompression operation
     * @return
     *          The decompressed data
     */
    byte[] decompress(byte[] data, int length, OpStatsLogger decompressionStat);

    /**
     * Return the decompressed data as a byte array.
     * @param data
     *          The data to the decompressed
     * @param length
     *          The number of bytes of data to decompress
     * @param decompressedSize
     *          The exact size of the decompressed data
     * @param decompressionStat
     *          The stat to use for timing the decompression operation
     * @return
     *          The decompressed data
     */
    byte[] decompress(byte[] data, int length, int decompressedSize, OpStatsLogger decompressionStat);
}
