package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.TransmitListener;

import java.io.IOException;

/**
 * Write representation of a {@link LogRecordSet}.
 * It is a buffer of log record set, used for transmission.
 */
public interface LogRecordSetBuffer extends TransmitListener {

    /**
     * Return if this record set contains user records.
     *
     * @return true if this record set contains user records, otherwise
     * return false.
     */
    boolean hasUserRecords();

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
     * Return max tx id in current record set.
     *
     * @return max tx id.
     */
    long getMaxTxId();

    /**
     * Get the buffer to transmit.
     *
     * @return the buffer to transmit.
     * @throws InvalidEnvelopedEntryException if the record set buffer is invalid
     * @throws IOException when encountered IOException during serialization
     */
    Buffer getBuffer() throws InvalidEnvelopedEntryException, IOException;

}
