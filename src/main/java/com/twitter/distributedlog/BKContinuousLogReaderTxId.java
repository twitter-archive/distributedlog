package com.twitter.distributedlog;

import java.io.IOException;

public class BKContinuousLogReaderTxId extends BKContinuousLogReaderBase implements LogReader {
    private final long startTxId;
    private long lastTxId;

    public BKContinuousLogReaderTxId(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     long startTxId,
                                     boolean readAheadEnabled,
                                     int readAheadWaitTime,
                                     boolean noBlocking) throws IOException {
        super(bkdlm, streamIdentifier, readAheadEnabled, readAheadWaitTime, noBlocking);
        this.startTxId = startTxId;
        lastTxId = startTxId - 1;
    }

    /**
     * Read the next log record from the stream
     *
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    @Override
    public LogRecordWithDLSN readNext(boolean shouldBlock) throws IOException {
        LogRecordWithDLSN record = super.readNext(shouldBlock);

        if (null != record) {
            lastTxId = record.getTransactionId();
        }

        return record;
    }

    @Override
    protected ResumableBKPerStreamLogReader getCurrentReader() throws IOException {
        LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));
        return bkLedgerManager.getInputStream(lastTxId + 1, true, false, (lastTxId >= startTxId), noBlocking);
    }
}
