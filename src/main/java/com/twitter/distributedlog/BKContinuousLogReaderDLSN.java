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
                                     AsyncNotification notification) throws IOException {
        super(bkdlm, streamIdentifier, readAheadEnabled, notification);
        this.startDLSN = startDLSN;
        lastDLSN = DLSN.InvalidDLSN;
    }

    /**
     * Read the next log record from the stream
     *
     * @param nonBlockingReadOperation should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    @Override
    public LogRecordWithDLSN readNext(boolean nonBlockingReadOperation) throws IOException {
        LogRecordWithDLSN record = super.readNext(nonBlockingReadOperation);

        if (null != record) {
            lastDLSN = record.getDlsn();
        }

        return record;
    }

    @Override
    protected ResumableBKPerStreamLogReader getCurrentReader() throws IOException {
        DLSN position = nextDLSN;
        if (DLSN.InvalidDLSN == nextDLSN) {
            if (DLSN.InvalidDLSN != lastDLSN) {
                position = new DLSN(lastDLSN.getLedgerSequenceNo() + 1, -1 , -1);
            } else {
                position = startDLSN;
            }
        }
        LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), position);
        return bkLedgerManager.getInputStream(position, (lastDLSN != DLSN.InvalidDLSN), simulateErrors);
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
