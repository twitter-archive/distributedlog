package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.io.Abortable;
import com.twitter.distributedlog.io.Abortables;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

abstract class BKBaseLogWriter implements Closeable, Abortable {
    static final Logger LOG = LoggerFactory.getLogger(BKBaseLogWriter.class);

    protected final BKDistributedLogManager bkDistributedLogManager;
    // Used by tests
    private Long minTimestampToKeepOverride = null;
    private boolean closed = false;
    private boolean forceRolling = false;
    private boolean forceRecovery = false;
    private LogTruncationTask lastTruncationAttempt = null;
    protected final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;

    public BKBaseLogWriter(DistributedLogConfiguration conf, DynamicDistributedLogConfiguration dynConf,
                           BKDistributedLogManager bkdlm) {
        this.conf = conf;
        this.dynConf = dynConf;
        this.bkDistributedLogManager = bkdlm;
        LOG.info("Initial retention period for {} : {}", bkdlm.getStreamName(),
                TimeUnit.MILLISECONDS.convert(dynConf.getRetentionPeriodHours(), TimeUnit.HOURS));
    }

    abstract protected BKLogPartitionWriteHandler getCachedPartitionHandler(String streamIdentifier);

    abstract protected void cachePartitionHandler(String streamIdentifier, BKLogPartitionWriteHandler ledgerHandler);

    abstract protected BKLogPartitionWriteHandler removeCachedPartitionHandler(String streamIdentifier);

    abstract protected Collection<BKLogPartitionWriteHandler> getCachedPartitionHandlers();

    abstract protected BKLogSegmentWriter getCachedLogWriter(String streamIdentifier);

    abstract protected void cacheLogWriter(String streamIdentifier, BKLogSegmentWriter logWriter);

    abstract protected BKLogSegmentWriter removeCachedLogWriter(String streamIdentifier);

    abstract protected Collection<BKLogSegmentWriter> getCachedLogWriters();

    abstract protected BKLogSegmentWriter getAllocatedLogWriter(String streamIdentifier);

    abstract protected void cacheAllocatedLogWriter(String streamIdentifier, BKLogSegmentWriter logWriter);

    abstract protected BKLogSegmentWriter removeAllocatedLogWriter(String streamIdentifier);

    abstract protected Collection<BKLogSegmentWriter> getAllocatedLogWriters();

    abstract protected void closeAndComplete(boolean shouldThrow) throws IOException;

    @VisibleForTesting
    void closeAndComplete() throws IOException {
        closeAndComplete(true);
    }

    @Override
    public void close() throws IOException {
        closeAndComplete(false);
    }

    /**
     * Close the writer and release all the underlying resources
     */
    protected void closeNoThrow() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        waitForTruncation();
        for (BKLogSegmentWriter writer : getCachedLogWriters()) {
            try {
                writer.close();
            } catch (IOException ioe) {
                LOG.error("Failed to close per stream writer : ", ioe);
            }
        }
        for (BKLogSegmentWriter writer : getAllocatedLogWriters()) {
            try {
                writer.close();
            } catch (IOException ioe) {
                LOG.error("Failed to close allocated per stream writer : ", ioe);
            }
        }
        for (BKLogPartitionWriteHandler partitionWriteHandler : getCachedPartitionHandlers()) {
            partitionWriteHandler.close();
        }
    }

    @Override
    public void abort() throws IOException {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        waitForTruncation();
        for (BKLogSegmentWriter writer : getCachedLogWriters()) {
            Abortables.abortQuietly(writer);
        }
        for (BKLogSegmentWriter writer : getAllocatedLogWriters()) {
            Abortables.abortQuietly(writer);
        }
        for (BKLogPartitionHandler writeHandler : getCachedPartitionHandlers()) {
            writeHandler.close();
        }
    }

    synchronized protected BKLogPartitionWriteHandler getWriteLedgerHandler(PartitionId partition, boolean recover) throws IOException {
        return getWriteLedgerHandler(partition.toString(), recover);
    }

    synchronized protected BKLogPartitionWriteHandler getWriteLedgerHandler(String streamIdentifier, boolean recover) throws IOException {
        BKLogPartitionWriteHandler ledgerManager = createAndCacheWriteHandler(streamIdentifier, null);
        ledgerManager.checkMetadataException();
        if (recover) {
            ledgerManager.recoverIncompleteLogSegments();
        }
        return ledgerManager;
    }

    synchronized protected BKLogPartitionWriteHandler createAndCacheWriteHandler(String streamIdentifier,
                                                                                 FuturePool orderedFuturePool)
            throws IOException {
        BKLogPartitionWriteHandler ledgerManager = getCachedPartitionHandler(streamIdentifier);
        if (null == ledgerManager) {
            ledgerManager = bkDistributedLogManager.createWriteLedgerHandler(streamIdentifier, orderedFuturePool);
            cachePartitionHandler(streamIdentifier, ledgerManager);
        }
        return ledgerManager;
    }

    synchronized protected BKLogSegmentWriter getLedgerWriter(PartitionId partition, long startTxId, boolean allowMaxTxID) throws IOException {
        return getLedgerWriter(partition.toString(), startTxId, allowMaxTxID);
    }

    synchronized protected BKLogSegmentWriter getLedgerWriter(String streamIdentifier, long startTxId, boolean allowMaxTxID)
            throws IOException {
        BKLogSegmentWriter ledgerWriter = getLedgerWriter(streamIdentifier);
        return rollLogSegmentIfNecessary(ledgerWriter, streamIdentifier, startTxId, true /* bestEffort */, allowMaxTxID);
    }

    synchronized protected BKLogSegmentWriter getLedgerWriter(String streamIdentifier) throws IOException {
        BKLogSegmentWriter ledgerWriter = getCachedLogWriter(streamIdentifier);

        // Handle the case where the last call to write actually caused an error in the partition
        //
        if ((null != ledgerWriter) && (ledgerWriter.isStreamInError() || forceRecovery)) {
            // Close the ledger writer so that we will recover and start a new log segment
            ledgerWriter.close();
            ledgerWriter = null;
            removeCachedLogWriter(streamIdentifier);

            // This is strictly not necessary - but its safe nevertheless
            BKLogPartitionWriteHandler ledgerManager = removeCachedPartitionHandler(streamIdentifier);
            if (null != ledgerManager) {
                ledgerManager.close();
            }
        }

        return ledgerWriter;
    }

    synchronized boolean shouldStartNewSegment(BKLogSegmentWriter ledgerWriter, String streamIdentifier) throws IOException {
        BKLogPartitionWriteHandler ledgerManager = getWriteLedgerHandler(streamIdentifier, false);
        return null == ledgerWriter || ledgerManager.shouldStartNewSegment(ledgerWriter) || forceRolling;
    }

    synchronized protected BKLogSegmentWriter rollLogSegmentIfNecessary(BKLogSegmentWriter ledgerWriter,
                                                                          String streamIdentifier, long startTxId,
                                                                          boolean bestEffort,
                                                                          boolean allowMaxTxID) throws IOException {
        boolean shouldCheckForTruncation = false;
        BKLogPartitionWriteHandler ledgerManager = getWriteLedgerHandler(streamIdentifier, false);
        if (null != ledgerWriter && (ledgerManager.shouldStartNewSegment(ledgerWriter) || forceRolling)) {
            PermitManager.Permit switchPermit = bkDistributedLogManager.getLogSegmentRollingPermitManager().acquirePermit();
            try {
                if (switchPermit.isAllowed()) {
                    try {
                        // we switch only when we could allocate a new log segment.
                        BKLogSegmentWriter newLedgerWriter = getAllocatedLogWriter(streamIdentifier);
                        if (null == newLedgerWriter) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Allocating a new log segment from {} for {}.", startTxId,
                                        ledgerManager.getFullyQualifiedName());
                            }
                            newLedgerWriter = ledgerManager.startLogSegment(startTxId, bestEffort, allowMaxTxID);
                            if (null == newLedgerWriter) {
                                assert (bestEffort);
                                return ledgerWriter;
                            }

                            cacheAllocatedLogWriter(streamIdentifier, newLedgerWriter);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Allocated a new log segment from {} for {}.", startTxId,
                                        ledgerManager.getFullyQualifiedName());
                            }
                        }

                        // complete the log segment
                        ledgerManager.completeAndCloseLogSegment(ledgerWriter);
                        ledgerWriter = newLedgerWriter;
                        cacheLogWriter(streamIdentifier, ledgerWriter);
                        removeAllocatedLogWriter(streamIdentifier);
                        shouldCheckForTruncation = true;
                    } catch (LockingException le) {
                        LOG.warn("We lost lock during completeAndClose log segment for {}. Disable ledger rolling until it is recovered : ",
                                ledgerManager.getFullyQualifiedName(), le);
                        bkDistributedLogManager.getLogSegmentRollingPermitManager().disallowObtainPermits(switchPermit);
                    } catch (ZKException zke) {
                        if (ZKException.isRetryableZKException(zke)) {
                            LOG.warn("Encountered zookeeper connection issues during completeAndClose log segment for {}." +
                                    " Disable ledger rolling until it is recovered : {}", ledgerManager.getFullyQualifiedName(),
                                    zke.getKeeperExceptionCode());
                            bkDistributedLogManager.getLogSegmentRollingPermitManager().disallowObtainPermits(switchPermit);
                        } else {
                            throw zke;
                        }
                    }
                }
            } finally {
                bkDistributedLogManager.getLogSegmentRollingPermitManager().releasePermit(switchPermit);
            }
        } else if (null == ledgerWriter) {
            // if exceptions thrown during initialize we should not catch it.
            ledgerWriter = getWriteLedgerHandler(streamIdentifier, true).startLogSegment(startTxId, false, allowMaxTxID);
            cacheLogWriter(streamIdentifier, ledgerWriter);
            shouldCheckForTruncation = true;
        }

        if (shouldCheckForTruncation) {
            boolean truncationEnabled = false;

            long minTimestampToKeep = 0;

            long retentionPeriodInMillis = TimeUnit.MILLISECONDS.convert(dynConf.getRetentionPeriodHours(), TimeUnit.HOURS);
            if (retentionPeriodInMillis > 0) {
                minTimestampToKeep = Utils.nowInMillis() - retentionPeriodInMillis;
                truncationEnabled = true;
            }

            if (null != minTimestampToKeepOverride) {
                minTimestampToKeep = minTimestampToKeepOverride;
                truncationEnabled = true;
            }

            // skip scheduling if there is task that's already running
            //
            if (truncationEnabled && ((lastTruncationAttempt == null) || lastTruncationAttempt.isDone())) {
                LogTruncationTask truncationTask = new LogTruncationTask(ledgerManager,
                        minTimestampToKeep);
                if (bkDistributedLogManager.scheduleTask(truncationTask)) {
                    lastTruncationAttempt = truncationTask;
                }
            }
        }

        return ledgerWriter;
    }

    protected synchronized void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (closed) {
            LOG.error("Executing " + operation + " on already closed Log Writer");
            throw new AlreadyClosedException("Executing " + operation + " on already closed Log Writer");
        }
    }

    static class LogTruncationTask implements Runnable, FutureEventListener<List<LogSegmentLedgerMetadata>> {
        private final BKLogPartitionWriteHandler ledgerManager;
        private final long minTimestampToKeep;
        private volatile boolean done = false;
        private volatile boolean running = false;
        private final CountDownLatch latch = new CountDownLatch(1);

        LogTruncationTask(BKLogPartitionWriteHandler ledgerManager, long minTimestampToKeep) {
            this.ledgerManager = ledgerManager;
            this.minTimestampToKeep = minTimestampToKeep;
        }

        boolean isDone() {
            return done;
        }

        @Override
        public void run() {
            LOG.info("Try to purge logs older than {} for {}.",
                     minTimestampToKeep, ledgerManager.getFullyQualifiedName());
            running = true;
            ledgerManager.purgeLogsOlderThanTimestamp(minTimestampToKeep)
                         .addEventListener(this);
        }

        public void waitForCompletion() throws InterruptedException {
            if (running) {
                latch.await();
            }
        }

        @Override
        public void onSuccess(List<LogSegmentLedgerMetadata> value) {
            LOG.info("Purged logs older than {} for {}.",
                     minTimestampToKeep, ledgerManager.getFullyQualifiedName());
            complete();
        }

        @Override
        public void onFailure(Throwable cause) {
            LOG.warn("Log Truncation {} Failed with exception : ", toString(), cause);
            complete();
        }

        void complete() {
            done = true;
            running = false;
            latch.countDown();
        }

        @Override
        public String toString() {
            return String.format("LogTruncationTask (%s : minTimestampToKeep=%d, running=%s, done=%s)",
                    ledgerManager.getFullyQualifiedName(), minTimestampToKeep, running, done);
        }
    }

    /**
     * All data that has been written to the stream so far will be flushed.
     * New data can be still written to the stream while flush is ongoing.
     */
    public long setReadyToFlush() throws IOException {
        checkClosedOrInError("setReadyToFlush");
        long highestTransactionId = 0;
        for (BKLogSegmentWriter writer : getCachedLogWriters()) {
            highestTransactionId = Math.max(highestTransactionId, writer.setReadyToFlush());
        }
        return highestTransactionId;
    }

    /**
     * Flush and sync all data that is ready to be flush
     * {@link #setReadyToFlush()} into underlying persistent store.
     * <p/>
     * This API is optional as the writer implements a policy for automatically syncing
     * the log records in the buffer. The buffered edits can be flushed when the buffer
     * becomes full or a certain period of time is elapsed.
     */
    public long flushAndSync() throws IOException {
        return flushAndSync(true, true);
    }

    public long flushAndSync(boolean parallel, boolean waitForVisibility) throws IOException {
        checkClosedOrInError("flushAndSync");

        LOG.debug("FlushAndSync Started");

        long highestTransactionId = 0;

        Collection<BKLogSegmentWriter> writerSet = getCachedLogWriters();

        if (parallel || !waitForVisibility) {
            for(BKLogSegmentWriter writer : writerSet) {
                highestTransactionId = Math.max(highestTransactionId, writer.commitPhaseOne());
            }
        }

        for(BKLogSegmentWriter writer : writerSet) {
            if (waitForVisibility) {
                if (parallel) {
                    highestTransactionId = Math.max(highestTransactionId, writer.commitPhaseTwo());
                } else {
                    highestTransactionId = Math.max(highestTransactionId, writer.flushAndSync());
                }
            }
        }

        if (writerSet.size() > 0) {
            LOG.debug("FlushAndSync Completed");
        } else {
            LOG.debug("FlushAndSync Completed - Nothing to Flush");
        }
        return highestTransactionId;
    }

    @VisibleForTesting
    public synchronized void setForceRolling(boolean forceRolling) {
        this.forceRolling = forceRolling;
    }

    @VisibleForTesting
    public synchronized void overRideMinTimeStampToKeep(Long minTimestampToKeepOverride) {
        this.minTimestampToKeepOverride = minTimestampToKeepOverride;
    }

    protected synchronized void waitForTruncation() {
        try {
            if (null != lastTruncationAttempt) {
                lastTruncationAttempt.waitForCompletion();
            }
        } catch (InterruptedException exc) {
            LOG.info("Wait For truncation failed", exc);
        }
    }

    @VisibleForTesting
    public synchronized void setForceRecovery(boolean forceRecovery) {
        this.forceRecovery = forceRecovery;
    }

}