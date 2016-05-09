/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
     * @param offset
     *          The offset in the bytes of data to compress
     * @param length
     *          The number of bytes of data to compress
     * @param compressionStat
     *          The stat to use for timing the compression operation
     * @return
     *          The compressed data
     *          The returned byte array is sized to the length of the compressed data
     */
    byte[] compress(byte[] data, int offset, int length, OpStatsLogger compressionStat);

    /**
     * Return the decompressed data as a byte array.
     * @param data
     *          The data to be decompressed
     * @param offset
     *          The offset in the bytes of data to decompress
     * @param length
     *          The number of bytes of data to decompress
     * @param decompressionStat
     *          The stat to use for timing the decompression operation
     * @return
     *          The decompressed data
     */
    byte[] decompress(byte[] data, int offset, int length, OpStatsLogger decompressionStat);

    /**
     * Return the decompressed data as a byte array.
     * @param data
     *          The data to the decompressed
     * @param offset
     *          The offset in the bytes of data to decompress
     * @param length
     *          The number of bytes of data to decompress
     * @param decompressedSize
     *          The exact size of the decompressed data
     * @param decompressionStat
     *          The stat to use for timing the decompression operation
     * @return
     *          The decompressed data
     */
    byte[] decompress(byte[] data, int offset, int length, int decompressedSize, OpStatsLogger decompressionStat);
}
