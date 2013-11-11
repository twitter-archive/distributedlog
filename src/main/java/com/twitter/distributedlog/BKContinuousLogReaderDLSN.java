package com.twitter.distributedlog;

import java.io.IOException;

public class BKContinuousLogReaderDLSN extends BKContinuousLogReaderBase implements LogReader {
    private final DLSN startDLSN;
    private DLSN lastDLSN;

    public BKContinuousLogReaderDLSN(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     DLSN startDLSN,
                                     boolean readAheadEnabled,
                                     int readAheadWaitTime) throws IOException {
        super(bkdlm, streamIdentifier, readAheadEnabled, readAheadWaitTime);
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
    protected boolean createOrPositionReader(boolean advancedOnce) throws IOException {
        if (null == currentReader) {
            DLSN nextDLSN = lastDLSN.getNextDLSN();
            LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), nextDLSN);
            currentReader = bkLedgerManager.getInputStream(nextDLSN, true, false, (lastDLSN != DLSN.InvalidDLSN));
            if (null != currentReader) {
                if(readAheadEnabled && bkLedgerManager.startReadAhead(currentReader.getNextLedgerEntryToRead())) {
                    bkLedgerManager.getLedgerDataAccessor().setReadAheadEnabled(true, readAheadWaitTime);
                }
                LOG.debug("Opened reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), nextDLSN);
            }
            advancedOnce = true;
        } else {
            currentReader.resume();
        }

        return advancedOnce;
    }
}
