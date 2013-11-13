package com.twitter.distributedlog;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKContinuousLogReaderDLSN extends BKContinuousLogReaderBase implements LogReader {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReaderDLSN.class);

    private final DLSN startDLSN;
    private DLSN lastDLSN;

    public BKContinuousLogReaderDLSN(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     DLSN startDLSN,
                                     boolean readAheadEnabled,
                                     int readAheadWaitTime,
                                     boolean noBlocking,
                                     AsyncNotification notification) throws IOException {
        super(bkdlm, streamIdentifier, readAheadEnabled, readAheadWaitTime, noBlocking, notification);
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
        DLSN nextDLSN = startDLSN;
        if (DLSN.InvalidDLSN != lastDLSN) {
            nextDLSN = lastDLSN.getNextDLSN();
        }
        LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), nextDLSN);
        return bkLedgerManager.getInputStream(nextDLSN, true, false, (lastDLSN != DLSN.InvalidDLSN), noBlocking, readAheadWaitTime);
    }

    @Override
    protected LogRecordWithDLSN readNextWithSkip() throws IOException {
        LogRecordWithDLSN record = null;
        while (true) {
            record = readNext(false);
            if ((null == record) || (record.getDlsn().compareTo(startDLSN) >= 0)) {
                return record;
            }
        }
    }
}
