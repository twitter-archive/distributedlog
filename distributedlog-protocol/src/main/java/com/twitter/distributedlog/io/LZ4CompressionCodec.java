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
    public byte[] compress(byte[] data, int offset, int length, OpStatsLogger compressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(offset >= 0 && offset < data.length);
        Preconditions.checkArgument(length >= 0);
        Preconditions.checkNotNull(compressionStat);

        Stopwatch watch = Stopwatch.createStarted();
        byte[] compressed = compressor.compress(data, offset, length);
        compressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
        return compressed;
    }

    @Override
    public byte[] decompress(byte[] data, int offset, int length, OpStatsLogger decompressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(offset >= 0 && offset < data.length);
        Preconditions.checkArgument(length >= 0);
        Preconditions.checkNotNull(decompressionStat);

        Stopwatch watch = Stopwatch.createStarted();
        // Assume that we have a compression ratio of 1/3.
        int outLength = length * 3;
        while (true) {
            try {
                byte[] decompressed = safeDecompressor.decompress(data, offset, length, outLength);
                decompressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
                return decompressed;
            } catch (LZ4Exception e) {
                outLength *= 2;
            }
        }
    }

    @Override
    // length parameter is ignored here because of the way the fastDecompressor works.
    public byte[] decompress(byte[] data, int offset, int length, int decompressedSize,
                             OpStatsLogger decompressionStat) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(offset >= 0 && offset < data.length);
        Preconditions.checkArgument(length >= 0);
        Preconditions.checkArgument(decompressedSize >= 0);
        Preconditions.checkNotNull(decompressionStat);

        Stopwatch watch = Stopwatch.createStarted();
        byte[] decompressed = fastDecompressor.decompress(data, offset, decompressedSize);
        decompressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
        return decompressed;
    }
}
