package com.twitter.distributedlog;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKContinuousLogReaderTxId extends BKContinuousLogReaderBase implements LogReader {
    static final Logger LOG = LoggerFactory.getLogger(BKContinuousLogReaderTxId.class);

    private final long startTxId;
    private long lastTxId;

    public BKContinuousLogReaderTxId(BKDistributedLogManager bkdlm,
                                     String streamIdentifier,
                                     long startTxId,
                                     final DistributedLogConfiguration conf,
                                     AsyncNotification notification) throws IOException {
        super(bkdlm, streamIdentifier, conf, notification);
        this.startTxId = startTxId;
        lastTxId = startTxId - 1;
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
            lastTxId = record.getTransactionId();
        }

        return record;
    }

    @Override
    protected LogRecordWithDLSN readNextWithSkip() throws IOException {
        LogRecordWithDLSN record = null;
        while (true) {
            record = readNext(false);
            if ((null == record) || (record.getTransactionId() >= startTxId)) {
                return record;
            }
        }
    }

    @Override
    protected ResumableBKPerStreamLogReader getCurrentReader() throws IOException {
        if (DLSN.InvalidDLSN == nextDLSN) {
            LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), (lastTxId + 1));
            return bkLedgerManager.getInputStream(lastTxId + 1, (lastTxId >= startTxId), simulateErrors);
        } else {
            LOG.debug("Opening reader on partition {} starting at TxId: {}", bkLedgerManager.getFullyQualifiedName(), nextDLSN);
            return bkLedgerManager.getInputStream(nextDLSN, true, simulateErrors);
        }
    }
}
