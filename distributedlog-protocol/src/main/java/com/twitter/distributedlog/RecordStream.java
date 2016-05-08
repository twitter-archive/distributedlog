package com.twitter.distributedlog;

/**
 * Stream of records
 */
public interface RecordStream {
    /**
     * advance <i>numRecords</i> records.
     */
    void advance(int numRecords);

    /**
     * Get postion of current record in the stream
     *
     * @return position of current record
     */
    DLSN getCurrentPosition();

    /**
     * Get the name of the stream
     *
     * @return the name of the stream
     */
    String getName();
}
