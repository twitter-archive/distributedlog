package com.twitter.distributedlog.v2.util;

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
