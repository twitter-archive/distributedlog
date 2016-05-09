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
