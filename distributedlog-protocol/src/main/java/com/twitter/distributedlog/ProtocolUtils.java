package com.twitter.distributedlog;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * With CRC embedded in the application, we have to keep track of per api crc. Ideally this
 * would be done by thrift.
 */
public class ProtocolUtils {

    static final Logger logger = LoggerFactory.getLogger(ProtocolUtils.class);

    /**
     * Generate crc32 for WriteOp.
     */
    public static Long writeOpCRC32(CRC32 crc, String stream, byte[] payload) {
        crc.update(stream.getBytes());
        crc.update(payload);
        long result = crc.getValue();
        crc.reset();
        return result;
    }

    /**
     * Generate crc32 for TruncateOp.
     */
    public static Long truncateOpCRC32(CRC32 crc, String stream, DLSN dlsn) {
        crc.update(stream.getBytes());
        crc.update(dlsn.serializeBytes());
        long result = crc.getValue();
        crc.reset();
        return result;
    }

    /**
     * Generate crc32 for any op which only passes a stream name.
     */
    public static Long streamOpCRC32(CRC32 crc, String stream) {
        crc.update(stream.getBytes());
        long result = crc.getValue();
        crc.reset();
        return result;
    }
}
