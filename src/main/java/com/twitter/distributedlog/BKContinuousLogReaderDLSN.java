package com.twitter.distributedlog;

import java.io.IOException;

public class BKContinuousLogReaderDLSN extends BKContinuousLogReaderBase implements LogReader {
    private final DLSN startDLSN;
    private DLSN lastDLSN;

    public BKContinuousLogReaderDLSN(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     DLSN startDLSN,
                                     boolean readAheadEnabled,
                                     int readAheadWaitTime,
                                     boolean noBlocking) throws IOException {
        super(bkdlm, streamIdentifier, readAheadEnabled, readAheadWaitTime, noBlocking);
        this.startDLSN = startDLSN;
        lastDLSN = DLSN.InvalidDLSN;
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
            lastDLSN = record.getDlsn();
        }

        return record;
    }

    @Override
    protected ResumableBKPerStreamLogReader getCurrentReader() throws IOException {
        DLSN nextDLSN = lastDLSN.getNextDLSN();
        LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), nextDLSN);
        return bkLedgerManager.getInputStream(nextDLSN, true, false, (lastDLSN != DLSN.InvalidDLSN), noBlocking);
    }
}
