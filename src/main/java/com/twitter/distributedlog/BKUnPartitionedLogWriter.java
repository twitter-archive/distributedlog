package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


public class BKUnPartitionedLogWriter extends BKBaseLogWriter implements LogWriter {
    private BKPerStreamLogWriter perStreamWriter = null;
    private BKLogPartitionWriteHandler partitionHander = null;


    public BKUnPartitionedLogWriter(DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) throws IOException {
        super(conf, bkdlm);
    }

    @Override
    protected BKLogPartitionWriteHandler getCachedPartitionHandler(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        return partitionHander;
    }

    // Since we have only one stream we simply maintain one partition handler and one ledger
    @Override
    protected void cachePartitionHandler(String streamIdentifier, BKLogPartitionWriteHandler ledgerHandler) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        partitionHander = ledgerHandler;
    }

    @Override
    protected BKLogPartitionWriteHandler removeCachedPartitionHandler(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        BKLogPartitionWriteHandler ret = partitionHander;
        partitionHander = null;
        return ret;
    }

    @Override
    protected Collection<BKLogPartitionWriteHandler> getCachedPartitionHandlers() {
        LinkedList<BKLogPartitionWriteHandler> list = new LinkedList<BKLogPartitionWriteHandler>();
        if (null != partitionHander) {
            list.add(partitionHander);
        }
        return list;
    }

    @Override
    protected BKPerStreamLogWriter getCachedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        return perStreamWriter;
    }

    @Override
    protected void cacheLogWriter(String streamIdentifier, BKPerStreamLogWriter logWriter) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        perStreamWriter = logWriter;
    }

    @Override
    protected BKPerStreamLogWriter removeCachedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        BKPerStreamLogWriter ret = perStreamWriter;
        perStreamWriter = null;
        return ret;
    }

    @Override
    protected Collection<BKPerStreamLogWriter> getCachedLogWriters() {
        LinkedList<BKPerStreamLogWriter> list = new LinkedList<BKPerStreamLogWriter>();
        if (null != perStreamWriter) {
            list.add(perStreamWriter);
        }
        return list;
    }


    /**
     * Write log records to the stream.
     *
     * @param record operation
     */
    @Override
    public void write(LogRecord record) throws IOException {
        if ((record.getTransactionId() < 0) ||
            (record.getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

        getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, record.getTransactionId()).write(record);
    }

    /**
     * Write edits logs operation to the stream.
     *
     * @param record list of records
     */
    @Override
    public int writeBulk(List<LogRecord> records) throws IOException {
        if ((records.get(0).getTransactionId() < 0) ||
            (records.get(0).getTransactionId() == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

        return getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM, records.get(0).getTransactionId()).writeBulk(records);
    }

    /**
     * Flushes all the data up to this point,
     * adds the end of stream marker and marks the stream
     * as read-only in the metadata. No appends to the
     * stream will be allowed after this point
     */
    @Override
    public void markEndOfStream() throws IOException {
        getLedgerWriter(DistributedLogConstants.DEFAULT_STREAM,
            DistributedLogConstants.MAX_TXID).markEndOfStream();
        closeAndComplete();
    }

    public void closeAndComplete() throws IOException {
        if (null != perStreamWriter && null != partitionHander) {
            try {
                waitForTruncation();
                long lastTxId = perStreamWriter.closeToFinalize();
                partitionHander.completeAndCloseLogSegment(lastTxId);
            } finally {
                // ensure partition handler is closed.
                partitionHander.close();
            }
            perStreamWriter = null;
            partitionHander = null;
        }
        close();
    }
}
