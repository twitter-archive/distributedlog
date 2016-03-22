package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.io.Abortable;
import com.twitter.distributedlog.io.Abortables;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

abstract class BKAbstractLogWriter implements Closeable, Abortable {
    static final Logger LOG = LoggerFactory.getLogger(BKAbstractLogWriter.class);

    protected final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    protected final BKDistributedLogManager bkDistributedLogManager;

    // States
    private boolean closed = false;
    private boolean forceRolling = false;
    private boolean forceRecovery = false;

    // Truncation Related
    private Future<List<LogSegmentMetadata>> lastTruncationAttempt = null;
    @VisibleForTesting
    private Long minTimestampToKeepOverride = null;

    // Log Segment Writers
    protected BKLogSegmentWriter segmentWriter = null;
    protected BKLogSegmentWriter allocatedSegmentWriter = null;
    protected BKLogWriteHandler writeHandler = null;

    BKAbstractLogWriter(DistributedLogConfiguration conf,
                        DynamicDistributedLogConfiguration dynConf,
                        BKDistributedLogManager bkdlm) {
        this.conf = conf;
        this.dynConf = dynConf;
        this.bkDistributedLogManager = bkdlm;
        LOG.debug("Initial retention period for {} : {}", bkdlm.getStreamName(),
                TimeUnit.MILLISECONDS.convert(dynConf.getRetentionPeriodHours(), TimeUnit.HOURS));
    }

    // manage write handler

    synchronized protected BKLogWriteHandler getCachedWriteHandler() {
        return writeHandler;
    }

    synchronized protected BKLogWriteHandler getWriteHandler() throws IOException {
        return createAndCacheWriteHandler(null).checkMetadataException();
    }

    synchronized protected BKLogWriteHandler createAndCacheWriteHandler(FuturePool orderedFuturePool)
            throws IOException {
        if (writeHandler != null) {
            return writeHandler;
        }
        writeHandler = bkDistributedLogManager.createWriteHandler(orderedFuturePool, false);
        return writeHandler;
    }

    // manage log segment writers

    protected BKLogSegmentWriter getCachedLogWriter() {
        return segmentWriter;
    }

    protected void cacheLogWriter(BKLogSegmentWriter logWriter) {
        this.segmentWriter = logWriter;
    }

    protected BKLogSegmentWriter removeCachedLogWriter() {
        try {
            return segmentWriter;
        } finally {
            segmentWriter = null;
        }
    }

    protected BKLogSegmentWriter getAllocatedLogWriter() {
        return allocatedSegmentWriter;
    }

    protected void cacheAllocatedLogWriter(BKLogSegmentWriter logWriter) {
        this.allocatedSegmentWriter = logWriter;
    }

    protected BKLogSegmentWriter removeAllocatedLogWriter() {
        try {
            return allocatedSegmentWriter;
        } finally {
            allocatedSegmentWriter = null;
        }
    }

    protected void closeAndComplete(boolean shouldThrow) throws IOException {
        try {
            if (null != segmentWriter && null != writeHandler) {
                writeHandler.completeAndCloseLogSegment(segmentWriter);
                segmentWriter = null;
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
        cancelTruncation();
        BKLogSegmentWriter writer = getCachedLogWriter();
        if (null != writer) {
            try {
                FutureUtils.result(writer.close());
            } catch (IOException ioe) {
                LOG.error("Failed to close per stream writer : ", ioe);
            }
        }
        writer = getAllocatedLogWriter();
        if (null != writer) {
            try {
                FutureUtils.result(writer.close());
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
        cancelTruncation();
        Abortables.abortQuietly(getCachedLogWriter());
        Abortables.abortQuietly(getAllocatedLogWriter());
        BKLogWriteHandler writeHandler = getCachedWriteHandler();
        if (null != writeHandler) {
            writeHandler.close();
        }
    }

    synchronized protected BKLogSegmentWriter getLedgerWriter(long startTxId, boolean allowMaxTxID)
            throws IOException {
        BKLogSegmentWriter ledgerWriter = getLedgerWriter( true);
        return rollLogSegmentIfNecessary(ledgerWriter, startTxId, true /* bestEffort */, allowMaxTxID);
    }

    synchronized protected BKLogSegmentWriter getLedgerWriter(boolean resetOnError) throws IOException {
        BKLogSegmentWriter ledgerWriter = getCachedLogWriter();

        // Handle the case where the last call to write actually caused an error in the log
        if ((null != ledgerWriter) && (ledgerWriter.isLogSegmentInError() || forceRecovery) && resetOnError) {
            BKLogSegmentWriter ledgerWriterToClose = ledgerWriter;
            // Close the ledger writer so that we will recover and start a new log segment
            if (ledgerWriterToClose.isLogSegmentInError()) {
                ledgerWriterToClose.abort();
            } else {
                FutureUtils.result(ledgerWriterToClose.close());
            }
            ledgerWriter = null;
            removeCachedLogWriter();

            if (!ledgerWriterToClose.isLogSegmentInError()) {
                BKLogWriteHandler writeHandler = getWriteHandler();
                if (null != writeHandler) {
                    writeHandler.completeAndCloseLogSegment(ledgerWriterToClose);
                }
            }
        }

        return ledgerWriter;
    }

    synchronized boolean shouldStartNewSegment(BKLogSegmentWriter ledgerWriter) throws IOException {
        BKLogWriteHandler ledgerManager = getWriteHandler();
        return null == ledgerWriter || ledgerManager.shouldStartNewSegment(ledgerWriter) || forceRolling;
    }

    private void truncateLogSegmentsIfNecessary(BKLogWriteHandler writeHandler) {
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
        if (truncationEnabled && ((lastTruncationAttempt == null) || lastTruncationAttempt.isDefined())) {
            lastTruncationAttempt = writeHandler.purgeLogSegmentsOlderThanTimestamp(minTimestampToKeep);
        }
    }

    private BKLogSegmentWriter startNewLogSegment(BKLogWriteHandler writeHandler,
                                                  long startTxId,
                                                  boolean allowMaxTxID)
            throws IOException {
        FutureUtils.result(writeHandler.recoverIncompleteLogSegments());
        // if exceptions thrown during initialize we should not catch it.
        BKLogSegmentWriter ledgerWriter = writeHandler.startLogSegment(startTxId, false, allowMaxTxID);
        cacheLogWriter(ledgerWriter);
        return ledgerWriter;
    }

    private BKLogSegmentWriter closeOldLogSegmentAndStartNewOne(BKLogSegmentWriter oldSegmentWriter,
                                                                BKLogWriteHandler writeHandler,
                                                                long startTxId,
                                                                boolean bestEffort,
                                                                boolean allowMaxTxID)
            throws IOException {
        // we switch only when we could allocate a new log segment.
        BKLogSegmentWriter newSegmentWriter = getAllocatedLogWriter();
        if (null == newSegmentWriter) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Allocating a new log segment from {} for {}.", startTxId,
                        writeHandler.getFullyQualifiedName());
            }
            newSegmentWriter = writeHandler.startLogSegment(startTxId, bestEffort, allowMaxTxID);
            if (null == newSegmentWriter) {
                assert (bestEffort);
                return oldSegmentWriter;
            }

            cacheAllocatedLogWriter(newSegmentWriter);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Allocated a new log segment from {} for {}.", startTxId,
                        writeHandler.getFullyQualifiedName());
            }
        }

        // complete the log segment
        writeHandler.completeAndCloseLogSegment(oldSegmentWriter);
        try {
            return newSegmentWriter;
        } finally {
            cacheLogWriter(newSegmentWriter);
            removeAllocatedLogWriter();
        }
    }

    synchronized protected BKLogSegmentWriter rollLogSegmentIfNecessary(BKLogSegmentWriter segmentWriter,
                                                                        long startTxId,
                                                                        boolean bestEffort,
                                                                        boolean allowMaxTxID)
            throws IOException {
        boolean shouldCheckForTruncation = false;
        BKLogWriteHandler writeHandler = getWriteHandler();
        if (null != segmentWriter && (writeHandler.shouldStartNewSegment(segmentWriter) || forceRolling)) {
            PermitManager.Permit switchPermit = bkDistributedLogManager.getLogSegmentRollingPermitManager().acquirePermit();
            try {
                if (switchPermit.isAllowed()) {
                    try {
                        segmentWriter = closeOldLogSegmentAndStartNewOne(
                                segmentWriter,
                                writeHandler,
                                startTxId,
                                bestEffort,
                                allowMaxTxID);
                        shouldCheckForTruncation = true;
                    } catch (LockingException le) {
                        LOG.warn("We lost lock during completeAndClose log segment for {}. Disable ledger rolling until it is recovered : ",
                                writeHandler.getFullyQualifiedName(), le);
                        bkDistributedLogManager.getLogSegmentRollingPermitManager().disallowObtainPermits(switchPermit);
                    } catch (ZKException zke) {
                        if (ZKException.isRetryableZKException(zke)) {
                            LOG.warn("Encountered zookeeper connection issues during completeAndClose log segment for {}." +
                                    " Disable ledger rolling until it is recovered : {}", writeHandler.getFullyQualifiedName(),
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
        } else if (null == segmentWriter) {
            segmentWriter = startNewLogSegment(writeHandler, startTxId, allowMaxTxID);
            shouldCheckForTruncation = true;
        }

        if (shouldCheckForTruncation) {
            truncateLogSegmentsIfNecessary(writeHandler);
        }
        return segmentWriter;
    }

    protected synchronized void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (closed) {
            LOG.error("Executing " + operation + " on already closed Log Writer");
            throw new AlreadyClosedException("Executing " + operation + " on already closed Log Writer");
        }
    }

    @VisibleForTesting
    public synchronized void setForceRolling(boolean forceRolling) {
        this.forceRolling = forceRolling;
    }

    @VisibleForTesting
    public synchronized void overRideMinTimeStampToKeep(Long minTimestampToKeepOverride) {
        this.minTimestampToKeepOverride = minTimestampToKeepOverride;
    }

    protected synchronized void cancelTruncation() {
        if (null != lastTruncationAttempt) {
            FutureUtils.cancel(lastTruncationAttempt);
            lastTruncationAttempt = null;
        }
    }

    @VisibleForTesting
    public synchronized void setForceRecovery(boolean forceRecovery) {
        this.forceRecovery = forceRecovery;
    }

}
