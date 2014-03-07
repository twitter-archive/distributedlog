package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BKPartitionAwareLogWriter extends BKBaseLogWriter implements PartitionAwareLogWriter {
    static final Logger LOG = LoggerFactory.getLogger(BKPartitionAwareLogWriter.class);
    private HashMap<String, BKPerStreamLogWriter> partitionToWriter;
    private HashMap<String, BKLogPartitionWriteHandler> partitionToLedger;

    public BKPartitionAwareLogWriter(DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) {
        super(conf, bkdlm);
        this.partitionToWriter = new HashMap<String, BKPerStreamLogWriter>();
        this.partitionToLedger = new HashMap<String, BKLogPartitionWriteHandler>();
    }

    @Override
    protected BKLogPartitionWriteHandler getCachedPartitionHandler(String streamIdentifier) {
        return partitionToLedger.get(streamIdentifier);
    }

    @Override
    protected void cachePartitionHandler(String streamIdentifier, BKLogPartitionWriteHandler ledgerHandler) {
        partitionToLedger.put(streamIdentifier, ledgerHandler);
    }

    @Override
    protected BKLogPartitionWriteHandler removeCachedPartitionHandler(String streamIdentifier) {
        return partitionToLedger.remove(streamIdentifier);
    }

    @Override
    protected Collection<BKLogPartitionWriteHandler> getCachedPartitionHandlers() {
        return partitionToLedger.values();
    }

    @Override
    protected BKPerStreamLogWriter getCachedLogWriter(String streamIdentifier) {
        return partitionToWriter.get(streamIdentifier);
    }

    @Override
    protected void cacheLogWriter(String streamIdentifier, BKPerStreamLogWriter logWriter) {
        partitionToWriter.put(streamIdentifier, logWriter);
    }

    @Override
    protected BKPerStreamLogWriter removeCachedLogWriter(String streamIdentifier) {
        return partitionToWriter.remove(streamIdentifier);
    }

    @Override
    protected Collection<BKPerStreamLogWriter> getCachedLogWriters() {
        return partitionToWriter.values();
    }

    /**
     * Write log records to the stream.
     *
     * @param record - the log record to be generated
     * @param partition – the partition to which this log record should be written
     */
    @Override
    public synchronized void write(LogRecord record, PartitionId partition) throws IOException {
        checkClosedOrInError("write");
        getLedgerWriter(partition, record.getTransactionId(), 1).write(record);
    }

    /**
     * Write log records to the stream.
     *
     * @param records – a map with a list of log records for one or more partitions
     */
    @Override
    public synchronized int writeBulk(Map<PartitionId, List<LogRecord>> records) throws IOException {
        checkClosedOrInError("writeBulk");
        int numRecords = 0;
        for (Map.Entry<PartitionId, List<LogRecord>> entry : records.entrySet()) {
            numRecords += getLedgerWriter(entry.getKey(), entry.getValue().get(0).getTransactionId(), records.size()).writeBulk(entry.getValue());
        }
        return numRecords;
    }

    @VisibleForTesting
    void closeAndComplete() throws IOException {
        closeAndComplete(true);
    }

    @Override
    protected void closeAndComplete(boolean shouldThrow) throws IOException {
        try {
            LinkedList<String> deletedStreams = new LinkedList<String>();
            for(String streamIdentifier: partitionToWriter.keySet()) {
                BKPerStreamLogWriter perStreamWriter = partitionToWriter.get(streamIdentifier);
                BKLogPartitionWriteHandler partitionHander = partitionToLedger.get(streamIdentifier);
                if (null != perStreamWriter && null != partitionHander) {
                    waitForTruncation();
                    partitionHander.completeAndCloseLogSegment(perStreamWriter);
                    partitionHander.close();
                    deletedStreams.add(streamIdentifier);
                }
            }
            for(String streamIdentifier: deletedStreams) {
                partitionToWriter.remove(streamIdentifier);
                partitionToLedger.remove(streamIdentifier);
            }
        } catch (IOException exc) {
            LOG.error("Completing Log segments encountered exception", exc);
            if (shouldThrow) {
                throw exc;
            }
        } finally {
            closeNoThrow();
        }
    }
}
