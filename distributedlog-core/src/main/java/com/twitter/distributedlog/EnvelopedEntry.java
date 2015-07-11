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
package com.twitter.distributedlog;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;

import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

import com.twitter.distributedlog.util.BitMaskUtils;
import com.twitter.distributedlog.util.CompressionCodec;
import com.twitter.distributedlog.util.CompressionUtils;
import com.twitter.distributedlog.util.DistributedLogAnnotations.Compression;

/**
 * An enveloped entry written to BookKeeper.
 *
 * Data type in brackets. Interpretation should be on the basis of data types and not individual
 * bytes to honor Endianness.
 *
 * Entry Structure:
 * ---------------
 * Bytes 0                                  : Version (Byte)
 * Bytes 1 - (DATA = 1+Header.length-1)     : Header (Integer)
 * Bytes DATA - DATA+3                      : Payload Length (Integer)
 * BYTES DATA+4 - DATA+4+payload.length-1   : Payload (Byte[])
 *
 * V1 Header Structure: // Offsets relative to the start of the header.
 * -------------------
 * Bytes 0 - 3                              : Flags (Integer)
 * Bytes 4 - 7                              : Original payload size before compression (Integer)
 *
 *      Flags: // 32 Bits
 *      -----
 *      0 ... 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
 *                                      |_|
 *                                       |
 *                               Compression Type
 *
 *      Compression Type: // 2 Bits (Least significant)
 *      ----------------
 *      00      : No Compression
 *      01      : LZ4 Compression
 *      10      : Unused
 *      11      : Unused
 */
public class EnvelopedEntry {

    public static final int VERSION_LENGTH = 1; // One byte long
    public static final byte VERSION_ONE = 1;

    public static final byte LOWEST_SUPPORTED_VERSION = VERSION_ONE;
    public static final byte HIGHEST_SUPPORTED_VERSION = VERSION_ONE;
    public static final byte CURRENT_VERSION = VERSION_ONE;

    private final OpStatsLogger compressionStat;
    private final OpStatsLogger decompressionStat;
    private final Counter compressedEntryBytes;
    private final Counter decompressedEntryBytes;
    private final byte version;

    private Header header = new Header();
    private Payload payloadCompressed = new Payload();
    private Payload payloadDecompressed = new Payload();

    public EnvelopedEntry(byte version,
                          StatsLogger statsLogger) throws InvalidEnvelopedEntryException {
        Preconditions.checkNotNull(statsLogger);
        if (version < LOWEST_SUPPORTED_VERSION || version > HIGHEST_SUPPORTED_VERSION) {
            throw new InvalidEnvelopedEntryException("Invalid enveloped entry version " + version + ", expected to be in [ "
                    + LOWEST_SUPPORTED_VERSION + " ~ " + HIGHEST_SUPPORTED_VERSION + " ]");
        }
        this.version = version;
        this.compressionStat = statsLogger.getOpStatsLogger("compression_time");
        this.decompressionStat = statsLogger.getOpStatsLogger("decompression_time");
        this.compressedEntryBytes = statsLogger.getCounter("compressed_bytes");
        this.decompressedEntryBytes = statsLogger.getCounter("decompressed_bytes");
    }

    /**
     * @param statsLogger
     *          Used for getting stats for (de)compression time
     * @param compressionType
     *          The compression type to use
     * @param decompressed
     *          The decompressed payload
     *          NOTE: The size of the byte array passed as the decompressed payload can be larger
     *                than the actual contents to be compressed.
     */
    public EnvelopedEntry(byte version,
                          CompressionCodec.Type compressionType,
                          byte[] decompressed,
                          int length,
                          StatsLogger statsLogger)
            throws InvalidEnvelopedEntryException {
        this(version, statsLogger);
        Preconditions.checkNotNull(compressionType);
        Preconditions.checkNotNull(decompressed);
        Preconditions.checkArgument(length >= 0, "Invalid bytes length " + length);

        this.header = new Header(compressionType, length);
        this.payloadDecompressed = new Payload(length, decompressed);
    }

    private boolean isReady() {
        return (header.ready && payloadDecompressed.ready);
    }

    @Compression
    public void writeFully(DataOutputStream out) throws IOException {
        Preconditions.checkNotNull(out);
        if (!isReady()) {
            throw new IOException("Entry not writable");
        }
        // Version
        out.writeByte(version);
        // Header
        header.write(out);
        // Compress
        CompressionCodec codec = CompressionUtils.getCompressionCodec(header.compressionType);
        byte[] compressed = codec.compress(payloadDecompressed.payload, payloadDecompressed.length,
                                           compressionStat);
        this.payloadCompressed = new Payload(compressed.length, compressed);
        this.compressedEntryBytes.add(payloadCompressed.length);
        this.decompressedEntryBytes.add(payloadDecompressed.length);
        payloadCompressed.write(out);
    }

    @Compression
    public void readFully(DataInputStream in) throws IOException {
        Preconditions.checkNotNull(in);
        // Make sure we're reading the right versioned entry.
        byte version = in.readByte();
        if (version != this.version) {
            throw new IOException(String.format("Version mismatch while reading. Received: %d," +
                    " Required: %d", version, this.version));
        }
        header.read(in);
        payloadCompressed.read(in);
        // Decompress
        CompressionCodec codec = CompressionUtils.getCompressionCodec(header.compressionType);
        byte[] decompressed = codec.decompress(payloadCompressed.payload, payloadCompressed.length,
                                               header.decompressedSize, decompressionStat);
        this.payloadDecompressed = new Payload(decompressed.length, decompressed);
        this.compressedEntryBytes.add(payloadCompressed.length);
        this.decompressedEntryBytes.add(payloadDecompressed.length);
    }

    public byte[] getDecompressedPayload() throws IOException {
        if (!isReady()) {
            throw new IOException("Decompressed payload is not initialized");
        }
        return payloadDecompressed.payload;
    }

    public static class Header {
        public static final int COMPRESSION_CODEC_MASK = 0x3;
        public static final int COMPRESSION_CODEC_NONE = 0x0;
        public static final int COMPRESSION_CODEC_LZ4 = 0x1;

        private int flags = 0;
        private int decompressedSize = 0;
        private CompressionCodec.Type compressionType = CompressionCodec.Type.UNKNOWN;

        // Whether this struct is ready for reading/writing.
        private boolean ready = false;

        // Used while reading.
        public Header() {
        }

        public Header(CompressionCodec.Type compressionType,
                      int decompressedSize) {
            this.compressionType = compressionType;
            this.decompressedSize = decompressedSize;
            this.flags = 0;
            switch (compressionType) {
                case NONE:
                    this.flags = (int) BitMaskUtils.set(flags, COMPRESSION_CODEC_MASK,
                                                        COMPRESSION_CODEC_NONE);
                    break;
                case LZ4:
                    this.flags = (int) BitMaskUtils.set(flags, COMPRESSION_CODEC_MASK,
                                                        COMPRESSION_CODEC_LZ4);
                    break;
                default:
                    throw new RuntimeException(String.format("Unknown Compression Type: %s",
                                                             compressionType));
            }
            // This can now be written.
            this.ready = true;
        }

        private void write(DataOutputStream out) throws IOException {
            out.writeInt(flags);
            out.writeInt(decompressedSize);
        }

        private void read(DataInputStream in) throws IOException {
            this.flags = in.readInt();
            int compressionType = (int) BitMaskUtils.get(flags, COMPRESSION_CODEC_MASK);
            if (compressionType == COMPRESSION_CODEC_NONE) {
                this.compressionType = CompressionCodec.Type.NONE;
            } else if (compressionType == COMPRESSION_CODEC_LZ4) {
                this.compressionType = CompressionCodec.Type.LZ4;
            } else {
                throw new IOException(String.format("Unsupported Compression Type: %s",
                                                    compressionType));
            }
            this.decompressedSize = in.readInt();
            // Values can now be read.
            this.ready = true;
        }
    }

    public static class Payload {
        private int length = 0;
        private byte[] payload = null;

        // Whether this struct is ready for reading/writing.
        private boolean ready = false;

        // Used for reading
        Payload() {
        }

        Payload(int length, byte[] payload) {
            this.length = length;
            this.payload = payload;
            this.ready = true;
        }

        private void write(DataOutputStream out) throws IOException {
            out.writeInt(length);
            out.write(payload, 0, length);
        }

        private void read(DataInputStream in) throws IOException {
            this.length = in.readInt();
            this.payload = new byte[length];
            in.readFully(payload);
            this.ready = true;
        }
    }

    /**
     * Return an InputStream that reads from the provided InputStream, decompresses the data
     * and returns a new InputStream wrapping the underlying payload.
     *
     * Note that src is modified by this call.
     *
     * @return
     *      New Input stream with the underlying payload.
     * @throws Exception
     */
    public static InputStream fromInputStream(InputStream src,
                                              StatsLogger statsLogger) throws IOException {
        src.mark(VERSION_LENGTH);
        byte version = new DataInputStream(src).readByte();
        src.reset();
        EnvelopedEntry entry = new EnvelopedEntry(version, statsLogger);
        entry.readFully(new DataInputStream(src));
        return new ByteArrayInputStream(entry.getDecompressedPayload());
    }

}
