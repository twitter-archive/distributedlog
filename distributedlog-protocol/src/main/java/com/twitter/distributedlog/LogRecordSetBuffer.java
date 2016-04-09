package com.twitter.distributedlog;

import java.nio.ByteBuffer;

/**
 * Write representation of a {@link LogRecordSet}.
 * It is a buffer of log record set, used for transmission.
 */
public interface LogRecordSetBuffer {

    /**
     * Return number of records in current record set.
     *
     * @return number of records in current record set.
     */
    int getNumRecords();

    /**
     * Return number of bytes in current record set.
     *
     * @return number of bytes in current record set.
     */
    int getNumBytes();

    /**
     * Get the buffer to transmit.
     *
     * @return the buffer to transmit.
     */
    ByteBuffer getBuffer();

    /**
     * Complete transmit.
     *
     * @param lssn log segment sequence number
     * @param entryId entry id
     * @param startSlotId start slot id
     */
    void completeTransmit(long lssn, long entryId, long startSlotId);

    /**
     * Abort transmit.
     *
     * @param reason reason to abort.
     */
    void abortTransmit(Throwable reason);

}
