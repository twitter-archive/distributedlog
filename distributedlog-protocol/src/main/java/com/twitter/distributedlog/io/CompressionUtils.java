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

public class CompressionUtils {

    public final static String LZ4 = "lz4";
    public final static String NONE = "none";

    private static CompressionCodec identityCodec = new IdentityCompressionCodec();
    private static CompressionCodec lz4Codec = new LZ4CompressionCodec();

    /**
     * Get a cached compression codec instance for the specified type.
     * @param type
     * @return
     */
    public static CompressionCodec getCompressionCodec(CompressionCodec.Type type) {
        if (type == CompressionCodec.Type.LZ4) {
            return lz4Codec;
        }
        // No Compression
        return identityCodec;
    }

    /**
     * Compression type value from string.
     * @param compressionString
     * @return
     */
    public static CompressionCodec.Type stringToType(String compressionString) {
        if (compressionString.equals(LZ4)) {
            return CompressionCodec.Type.LZ4;
        } else if (compressionString.equals(NONE)) {
            return CompressionCodec.Type.NONE;
        } else {
            return CompressionCodec.Type.UNKNOWN;
        }
    }
}
