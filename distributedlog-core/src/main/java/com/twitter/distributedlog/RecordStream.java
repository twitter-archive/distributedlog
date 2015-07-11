package com.twitter.distributedlog;

/**
 * Stream of records
 */
interface RecordStream {
    /**
     * Move to next record
     */
    void advanceToNextRecord();

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
