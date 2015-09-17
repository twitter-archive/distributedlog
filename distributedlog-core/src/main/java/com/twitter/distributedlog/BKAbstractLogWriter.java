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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

abstract class BKAbstractLogWriter implements Closeable, Abortable {
    static final Logger LOG = LoggerFactory.getLogger(BKAbstractLogWriter.class);

    protected final BKDistributedLogManager bkDistributedLogManager;
    // Used by tests
    private Long minTimestampToKeepOverride = null;
    private boolean closed = false;
    private boolean forceRolling = false;
    private boolean forceRecovery = false;
    private LogTruncationTask lastTruncationAttempt = null;
    protected final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;

    // Log Segment Writers
    protected BKLogSegmentWriter perStreamWriter = null;
    protected BKLogSegmentWriter allocatedPerStreamWriter = null;
    protected BKLogWriteHandler writeHandler = null;

    public BKAbstractLogWriter(DistributedLogConfiguration conf,
                               DynamicDistributedLogConfiguration dynConf,
                               BKDistributedLogManager bkdlm) {
        this.conf = conf;
        this.dynConf = dynConf;
        this.bkDistributedLogManager = bkdlm;
        LOG.info("Initial retention period for {} : {}", bkdlm.getStreamName(),
                TimeUnit.MILLISECONDS.convert(dynConf.getRetentionPeriodHours(), TimeUnit.HOURS));
    }

    protected BKLogWriteHandler getCachedWriteHandler() {
        return writeHandler;
    }

    protected void cacheWriteHandler(BKLogWriteHandler ledgerHandler) {
        this.writeHandler = ledgerHandler;
    }

    protected BKLogWriteHandler removeCachedWriteHandler() {
        try {
            return writeHandler;
        } finally {
            writeHandler = null;
        }
    }

    protected BKLogSegmentWriter getCachedLogWriter() {
        return perStreamWriter;
    }

    protected void cacheLogWriter(BKLogSegmentWriter logWriter) {
        this.perStreamWriter = logWriter;
    }

    protected BKLogSegmentWriter removeCachedLogWriter() {
        try {
            return perStreamWriter;
        } finally {
            perStreamWriter = null;
        }
    }

    protected BKLogSegmentWriter getAllocatedLogWriter() {
        return allocatedPerStreamWriter;
    }

    protected void cacheAllocatedLogWriter(BKLogSegmentWriter logWriter) {
        this.allocatedPerStreamWriter = logWriter;
    }

    protected BKLogSegmentWriter removeAllocatedLogWriter() {
        try {
            return allocatedPerStreamWriter;
        } finally {
            allocatedPerStreamWriter = null;
        }
    }

    protected void closeAndComplete(boolean shouldThrow) throws IOException {
        try {
            if (null != perStreamWriter && null != writeHandler) {
                try {
                    waitForTruncation();
                    writeHandler.completeAndCloseLogSegment(perStreamWriter);
                } finally {
                    // ensure write handler is closed
                    writeHandler.close();
                }
                perStreamWriter = null;
                writeHandler = null;
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
        BKLogSegmentWriter writer = getCachedLogWriter();
        if (null != writer) {
            try {
                writer.close();
            } catch (IOException ioe) {
                LOG.error("Failed to close per stream writer : ", ioe);
            }
        }
        writer = getAllocatedLogWriter();
        if (null != writer) {
            try {
                writer.close();
            } catch (IOException ioe) {
                LOG.error("Failed to close allocated per stream writer : ", ioe);
            }
        }
        BKLogWriteHandler writeHandler = getCachedWriteHandler();
        if (null != writeHandler) {
            writeHandler.close();
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
        Abortables.abortQuietly(getCachedLogWriter());
        Abortables.abortQuietly(getAllocatedLogWriter());
        BKLogWriteHandler writeHandler = getCachedWriteHandler();
        if (null != writeHandler) {
            writeHandler.close();
        }
    }

    synchronized protected BKLogWriteHandler getWriteLedgerHandler(String streamIdentifier) throws IOException {
        BKLogWriteHandler ledgerManager = createAndCacheWriteHandler(streamIdentifier, null);
        ledgerManager.checkMetadataException();
        return ledgerManager;
    }

    synchronized protected BKLogWriteHandler createAndCacheWriteHandler(String streamIdentifier,
                                                                                 FuturePool orderedFuturePool)
            throws IOException {
        BKLogWriteHandler ledgerManager = getCachedWriteHandler();
        if (null == ledgerManager) {
            ledgerManager = bkDistributedLogManager.createWriteLedgerHandler(streamIdentifier, orderedFuturePool);
            cacheWriteHandler(ledgerManager);
        }
        return ledgerManager;
    }

    synchronized protected BKLogSegmentWriter getLedgerWriter(String streamIdentifier, long startTxId, boolean allowMaxTxID)
            throws IOException {
        BKLogSegmentWriter ledgerWriter = getLedgerWriter(streamIdentifier);
        return rollLogSegmentIfNecessary(ledgerWriter, streamIdentifier, startTxId, true /* bestEffort */, allowMaxTxID);
    }

    synchronized protected BKLogSegmentWriter getLedgerWriter(String streamIdentifier) throws IOException {
        BKLogSegmentWriter ledgerWriter = getCachedLogWriter();

        // Handle the case where the last call to write actually caused an error in the log
        //
        if ((null != ledgerWriter) && (ledgerWriter.isStreamInError() || forceRecovery)) {
            // Close the ledger writer so that we will recover and start a new log segment
            ledgerWriter.close();
            ledgerWriter = null;
            removeCachedLogWriter();

            // This is strictly not necessary - but its safe nevertheless
            BKLogWriteHandler ledgerManager = removeCachedWriteHandler();
            if (null != ledgerManager) {
                ledgerManager.close();
            }
        }

        return ledgerWriter;
    }

    synchronized boolean shouldStartNewSegment(BKLogSegmentWriter ledgerWriter, String streamIdentifier) throws IOException {
        BKLogWriteHandler ledgerManager = getWriteLedgerHandler(streamIdentifier);
        return null == ledgerWriter || ledgerManager.shouldStartNewSegment(ledgerWriter) || forceRolling;
    }

    synchronized protected BKLogSegmentWriter rollLogSegmentIfNecessary(BKLogSegmentWriter ledgerWriter,
                                                                          String streamIdentifier, long startTxId,
                                                                          boolean bestEffort,
                                                                          boolean allowMaxTxID) throws IOException {
        boolean shouldCheckForTruncation = false;
        BKLogWriteHandler ledgerManager = getWriteLedgerHandler(streamIdentifier);
        if (null != ledgerWriter && (ledgerManager.shouldStartNewSegment(ledgerWriter) || forceRolling)) {
            PermitManager.Permit switchPermit = bkDistributedLogManager.getLogSegmentRollingPermitManager().acquirePermit();
            try {
                if (switchPermit.isAllowed()) {
                    try {
                        // we switch only when we could allocate a new log segment.
                        BKLogSegmentWriter newLedgerWriter = getAllocatedLogWriter();
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

                            cacheAllocatedLogWriter(newLedgerWriter);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Allocated a new log segment from {} for {}.", startTxId,
                                        ledgerManager.getFullyQualifiedName());
                            }
                        }

                        // complete the log segment
                        ledgerManager.completeAndCloseLogSegment(ledgerWriter);
                        ledgerWriter = newLedgerWriter;
                        cacheLogWriter(ledgerWriter);
                        removeAllocatedLogWriter();
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
            // recover incomplete log segments if no writer
            BKLogWriteHandler writeHandler = getWriteLedgerHandler(streamIdentifier);
            writeHandler.recoverIncompleteLogSegments();
            // if exceptions thrown during initialize we should not catch it.
            ledgerWriter = writeHandler.startLogSegment(startTxId, false, allowMaxTxID);
            cacheLogWriter(ledgerWriter);
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

    static class LogTruncationTask implements Runnable, FutureEventListener<List<LogSegmentMetadata>> {
        private final BKLogWriteHandler ledgerManager;
        private final long minTimestampToKeep;
        private volatile boolean done = false;
        private volatile boolean running = false;
        private final CountDownLatch latch = new CountDownLatch(1);

        LogTruncationTask(BKLogWriteHandler ledgerManager, long minTimestampToKeep) {
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
        public void onSuccess(List<LogSegmentMetadata> value) {
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
        BKLogSegmentWriter writer = getCachedLogWriter();
        if (null != writer) {
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

        BKLogSegmentWriter writer = getCachedLogWriter();

        if (parallel || !waitForVisibility) {
            if (null != writer) {
                highestTransactionId = Math.max(highestTransactionId, writer.commitPhaseOne());
            }
        }

        if (null != writer) {
            if (waitForVisibility) {
                if (parallel) {
                    highestTransactionId = Math.max(highestTransactionId, writer.commitPhaseTwo());
                } else {
                    highestTransactionId = Math.max(highestTransactionId, writer.flushAndSync());
                }
            }
        }

        if (null != writer) {
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
