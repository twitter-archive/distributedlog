package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BKPartitionAwareLogWriter implements PartitionAwareLogWriter, ZooKeeperClient.ZooKeeperSessionExpireNotifier {
    static final Logger LOG = LoggerFactory.getLogger(BKPartitionAwareLogWriter.class);

    private final BKDistributedLogManager bkDistributedLogManager;
    private HashMap<PartitionId, BKPerStreamLogWriter> partitionToWriter;
    private HashMap<PartitionId, BKLogPartitionWriteHandler> partitionToLedger;
    private final long retentionPeriodInMillis;
    // Used by tests
    private Long minTimestampToKeepOverride = null;
    private boolean closed = false;
    private boolean forceRolling = false;
    private boolean forceRecovery = false;
    private Future<?> lastTruncationAttempt = null;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;

    public BKPartitionAwareLogWriter (DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) {
        this.bkDistributedLogManager = bkdlm;
        this.partitionToWriter = new HashMap<PartitionId, BKPerStreamLogWriter>();
        this.partitionToLedger = new HashMap<PartitionId, BKLogPartitionWriteHandler>();
        this.retentionPeriodInMillis = (long)(conf.getRetentionPeriodHours()) * 3600 * 1000;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
        LOG.info("Retention Period {}", retentionPeriodInMillis);
    }

    /**
     * Close the journal.
     * @throws java.io.IOException if the log stream can't be closed,
     */
    @Override
    public void close() throws IOException {
        closed = true;
        waitForTruncation();
        for(Map.Entry<PartitionId, BKPerStreamLogWriter> entry : partitionToWriter.entrySet()) {
            entry.getValue().close();
        }
        for (Map.Entry<PartitionId,BKLogPartitionWriteHandler> entry : partitionToLedger.entrySet()) {
            entry.getValue().close();
        }
        bkDistributedLogManager.unregister(sessionExpireWatcher);
    }

    synchronized private BKLogPartitionWriteHandler getWriteLedgerHandler(PartitionId partition, boolean recover) throws IOException {
        BKLogPartitionWriteHandler ledgerManager = partitionToLedger.get(partition);
        if (null == ledgerManager) {
            ledgerManager = bkDistributedLogManager.createWriteLedgerHandler(partition);
            partitionToLedger.put(partition, ledgerManager);
        }
        if (recover) {
            ledgerManager.recoverIncompleteLogSegments();
        }
        return ledgerManager;
    }

    synchronized private BKPerStreamLogWriter getLedgerWriter(PartitionId partition) throws IOException {
        return partitionToWriter.get(partition);
    }

    synchronized private BKPerStreamLogWriter getLedgerWriter (PartitionId partition, long startTxId) throws IOException {
        BKPerStreamLogWriter ledgerWriter = partitionToWriter.get(partition);
        long numFlushes = 0;

        // Handle the case where the last call to write actually caused an error in the partition
        //
        if ((null != ledgerWriter) && (ledgerWriter.isStreamInError() || forceRecovery)) {
            // Close the ledger writer so that we will recover and start a new log segment
            numFlushes = ledgerWriter.getNumFlushes();
            ledgerWriter.close();
            ledgerWriter = null;
            partitionToWriter.remove(partition);

            // This is strictly not necessary - but its safe nevertheless
            BKLogPartitionWriteHandler ledgerManager = partitionToLedger.remove(partition);
            if (null != ledgerManager) {
                ledgerManager.close();
            }
        }


        if (null == ledgerWriter) {
            ledgerWriter = getWriteLedgerHandler(partition, true).startLogSegment(startTxId);
            ledgerWriter.setNumFlushes(numFlushes);
            partitionToWriter.put(partition, ledgerWriter);
        }

        BKLogPartitionWriteHandler ledgerManager = getWriteLedgerHandler(partition, false);
        if (ledgerManager.shouldStartNewSegment() || forceRolling) {
            long lastTxId = ledgerWriter.closeToFinalize();
            numFlushes = ledgerWriter.getNumFlushes();
            ledgerManager.completeAndCloseLogSegment(lastTxId);
            ledgerWriter = ledgerManager.startLogSegment(startTxId);
            ledgerWriter.setNumFlushes(numFlushes);
            partitionToWriter.put(partition, ledgerWriter);

            long minTimestampToKeep = Utils.nowInMillis() - retentionPeriodInMillis;
            long sanityCheckThreshold = Utils.nowInMillis() - 2 * retentionPeriodInMillis;

            if (null != minTimestampToKeepOverride) {
                minTimestampToKeep = minTimestampToKeepOverride;
            }

            lastTruncationAttempt = bkDistributedLogManager.enqueueBackgroundTask(
                                        new LogTruncationTask(ledgerManager,
                                            minTimestampToKeep,
                                            sanityCheckThreshold));

        }
        return ledgerWriter;
    }


    /**
     * Write log records to the stream.
     *
     * @param record - the log record to be generated
     * @param partition – the partition to which this log record should be written
     * @throws IOException
     */
    @Override
    public synchronized void write(LogRecord record, PartitionId partition) throws IOException {
        checkClosedOrInError("write");
        getLedgerWriter(partition, record.getTransactionId()).write(record);
    }

    /**
     * Write log records to the stream.
     *
     * @param records – a map with a list of log records for one or more partitions
     * @throws IOException
     */
    @Override
    public synchronized int writeBulk(Map<PartitionId, List<LogRecord>> records) throws IOException {
        checkClosedOrInError("writeBulk");
        int numRecords = 0;
        for (Map.Entry<PartitionId, List<LogRecord>> entry : records.entrySet()) {
            numRecords += getLedgerWriter(entry.getKey(), entry.getValue().get(0).getTransactionId()).writeBulk(entry.getValue());
        }
        return numRecords;
    }

    /**
     * All data that has been written to the stream so far will be flushed.
     * New data can be still written to the stream while flush is ongoing.
     */
    @Override
    public long setReadyToFlush() throws IOException {
        checkClosedOrInError("setReadyToFlush");
        long highestTransactionId = 0;
        for(Map.Entry<PartitionId, BKPerStreamLogWriter> entry : partitionToWriter.entrySet()) {
            highestTransactionId = Math.max(highestTransactionId, entry.getValue().setReadyToFlush());
        }
        return highestTransactionId;
    }

    @Override
    public long flushAndSync() throws IOException {
        return flushAndSync(true, true);
    }


    /**
     * Flush and sync all data that is ready to be flush
     * {@link #setReadyToFlush()} into underlying persistent store.
     *
     * This API is optional as the writer implements a policy for automatically syncing
     * the log records in the buffer. The buffered edits can be flushed when the buffer
     * becomes full or a certain period of time is elapsed.
     *
     * @throws IOException
     */
    @Override
    public long flushAndSync(boolean parallel, boolean waitForVisibility) throws IOException {
        checkClosedOrInError("flushAndSync");

        LOG.info("FlushAndSync Started");

        long highestTransactionId = 0;
        long totalFlushes = 0;
        long minTransmitSize = Long.MAX_VALUE;
        long maxTransmitSize = Long.MIN_VALUE;
        long totalAddConfirmed = 0;

        Collection<Map.Entry<PartitionId, BKPerStreamLogWriter>> entrySet = partitionToWriter.entrySet();

        if (parallel || !waitForVisibility) {
            for(Map.Entry<PartitionId, BKPerStreamLogWriter> entry : entrySet) {
                highestTransactionId = Math.max(highestTransactionId, entry.getValue().flushAndSyncPhaseOne());
            }
        }

        for(Map.Entry<PartitionId, BKPerStreamLogWriter> entry : entrySet) {
            if (waitForVisibility) {
                if (parallel) {
                    highestTransactionId = Math.max(highestTransactionId, entry.getValue().flushAndSyncPhaseTwo());
                } else {
                    highestTransactionId = Math.max(highestTransactionId, entry.getValue().flushAndSync());
                }
            }
            totalFlushes += entry.getValue().getNumFlushes();
            minTransmitSize = Math.min(minTransmitSize, entry.getValue().getAverageTransmitSize());
            maxTransmitSize = Math.max(maxTransmitSize, entry.getValue().getAverageTransmitSize());
            totalAddConfirmed += entry.getValue().getLastAddConfirmed();
        }

        if (entrySet.size() > 0) {
            LOG.info("FlushAndSync Completed with {} flushes and add Confirmed {}", totalFlushes, totalAddConfirmed);
            LOG.info("Transmission Size Min {} Max {}", minTransmitSize, maxTransmitSize);
        } else {
            LOG.info("FlushAndSync Completed - Nothing to Flush");
        }
        return highestTransactionId;
    }

    private void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (zkSessionExpired) {
            LOG.error("Executing " + operation + " after losing connection to zookeeper");
            throw new AlreadyClosedException("Executing " + operation + " after losing connection to zookeeper");
        }

        if (closed) {
            LOG.error("Executing " + operation + " on already closed Log Writer");
            throw new AlreadyClosedException("Executing " + operation + " on already closed Log Writer");
        }
    }

    class LogTruncationTask implements Runnable {
        private final BKLogPartitionWriteHandler ledgerManager;
        private final long minTimestampToKeep;
        private final long sanityCheckThreshold;

        LogTruncationTask(BKLogPartitionWriteHandler ledgerManager, long minTimestampToKeep, long sanityCheckThreshold) {
            this.ledgerManager = ledgerManager;
            this.minTimestampToKeep = minTimestampToKeep;
            this.sanityCheckThreshold = sanityCheckThreshold;
        }

        @Override
        public void run() {
            try {
                ledgerManager.purgeLogsOlderThanTimestamp(minTimestampToKeep, sanityCheckThreshold);
            } catch (IOException ioexc) {
                // One of the operations in the distributed log failed.
                LOG.warn("Log Truncation Failed with exception", ioexc);
            } catch (Exception e) {
                // Something unexpected happened
                LOG.error("Log Truncation Failed with exception", e);
            }
        }
    }

    public void setForceRolling(boolean forceRolling) {
         this.forceRolling = forceRolling;
    }

    public void overRideMinTimeStampToKeep(Long minTimestampToKeepOverride) {
        this.minTimestampToKeepOverride = minTimestampToKeepOverride;
    }

    private void waitForTruncation() {
        try {
        if (null != lastTruncationAttempt) {
            assert (null == lastTruncationAttempt.get());
        }
        } catch (Exception exc) {
            LOG.info("Wait For truncation failed", exc);
        }
    }

    public void setForceRecovery(boolean forceRecovery) {
        this.forceRecovery = forceRecovery;
    }

    @Override
    public void notifySessionExpired() {
        zkSessionExpired = true;
    }
}
