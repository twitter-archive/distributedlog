package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;

public class LogRecordWithDLSN extends LogRecord {
    private final DLSN dlsn;
    private final long startSequenceIdOfCurrentSegment;

    /**
     * This empty constructor can only be called from Reader#readOp.
     */
    LogRecordWithDLSN(DLSN dlsn, long startSequenceIdOfCurrentSegment) {
        super();
        this.dlsn = dlsn;
        this.startSequenceIdOfCurrentSegment = startSequenceIdOfCurrentSegment;
    }

    @VisibleForTesting
    LogRecordWithDLSN(DLSN dlsn, long txid, byte[] data, long startSequenceIdOfCurrentSegment) {
        super(txid, data);
        this.dlsn = dlsn;
        this.startSequenceIdOfCurrentSegment = startSequenceIdOfCurrentSegment;
    }

    /**
     * Sequence Id of the record in the stream. Current sequence id is generated as negative number
     * based on ledger sequence id. It isn't monotonic increasing.
     *
     * @return sequence id of the record in the stream.
     */
    public long getSequenceId() {
        return startSequenceIdOfCurrentSegment + getPositionWithinLogSegment() - 1;
    }

    public DLSN getDlsn() {
        return dlsn;
    }

    @Override
    public String toString() {
        return "LogRecordWithDLSN{" +
            "dlsn=" + dlsn +
            ", txid=" + getTransactionId() +
            ", position=" + getPositionWithinLogSegment() +
            ", isControl=" + isControl() +
            ", isEndOfStream=" + isEndOfStream() +
            '}';
    }
}
