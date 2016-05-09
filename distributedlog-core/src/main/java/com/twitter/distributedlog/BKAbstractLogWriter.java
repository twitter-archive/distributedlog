/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.AlreadyClosedException;
import com.twitter.distributedlog.exceptions.LockingException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.io.Abortable;
import com.twitter.distributedlog.io.Abortables;
import com.twitter.distributedlog.io.AsyncAbortable;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

abstract class BKAbstractLogWriter implements Closeable, AsyncCloseable, Abortable, AsyncAbortable {
    static final Logger LOG = LoggerFactory.getLogger(BKAbstractLogWriter.class);

    protected final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    protected final BKDistributedLogManager bkDistributedLogManager;

    // States
    private Promise<Void> closePromise = null;
    private boolean forceRolling = false;
    private boolean forceRecovery = false;

    // Truncation Related
    private Future<List<LogSegmentMetadata>> lastTruncationAttempt = null;
    @VisibleForTesting
    private Long minTimestampToKeepOverride = null;

    // Log Segment Writers
    protected BKLogSegmentWriter segmentWriter = null;
    protected Future<BKLogSegmentWriter> segmentWriterFuture = null;
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

    protected BKLogWriteHandler getWriteHandler() throws IOException {
        BKLogWriteHandler writeHandler = createAndCacheWriteHandler();
        writeHandler.checkMetadataException();
        return writeHandler;
    }

    protected BKLogWriteHandler createAndCacheWriteHandler()
            throws IOException {
        synchronized (this) {
            if (writeHandler != null) {
                return writeHandler;
            }
        }
        // This code path will be executed when the handler is not set or has been closed
        // due to forceRecovery during testing
        BKLogWriteHandler newHandler =
                FutureUtils.result(bkDistributedLogManager.asyncCreateWriteHandler(false));
        boolean success = false;
        try {
            synchronized (this) {
                if (writeHandler == null) {
                    writeHandler = newHandler;
                    success = true;
                }
                return writeHandler;
            }
        } finally {
            if (!success) {
                newHandler.asyncAbort();
            }
        }
    }

    // manage log segment writers

    protected synchronized BKLogSegmentWriter getCachedLogWriter() {
        return segmentWriter;
    }

    protected synchronized Future<BKLogSegmentWriter> getCachedLogWriterFuture() {
        return segmentWriterFuture;
    }

    protected synchronized void cacheLogWriter(BKLogSegmentWriter logWriter) {
        this.segmentWriter = logWriter;
        this.segmentWriterFuture = Future.value(logWriter);
    }

    protected synchronized BKLogSegmentWriter removeCachedLogWriter() {
        try {
            return segmentWriter;
        } finally {
            segmentWriter = null;
            segmentWriterFuture = null;
        }
    }

    protected synchronized BKLogSegmentWriter getAllocatedLogWriter() {
        return allocatedSegmentWriter;
    }

    protected synchronized void cacheAllocatedLogWriter(BKLogSegmentWriter logWriter) {
        this.allocatedSegmentWriter = logWriter;
    }

    protected synchronized BKLogSegmentWriter removeAllocatedLogWriter() {
        try {
            return allocatedSegmentWriter;
        } finally {
            allocatedSegmentWriter = null;
        }
    }

    private Future<Void> asyncCloseAndComplete(boolean shouldThrow) {
        BKLogSegmentWriter segmentWriter = getCachedLogWriter();
        BKLogWriteHandler writeHandler = getCachedWriteHandler();
        if (null != segmentWriter && null != writeHandler) {
            cancelTruncation();
            Promise<Void> completePromise = new Promise<Void>();
            asyncCloseAndComplete(segmentWriter, writeHandler, completePromise, shouldThrow);
            return completePromise;
        } else {
            return closeNoThrow();
        }
    }

    private void asyncCloseAndComplete(final BKLogSegmentWriter segmentWriter,
                                       final BKLogWriteHandler writeHandler,
                                       final Promise<Void> completePromise,
                                       final boolean shouldThrow) {
        writeHandler.completeAndCloseLogSegment(segmentWriter)
                .addEventListener(new FutureEventListener<LogSegmentMetadata>() {
                    @Override
                    public void onSuccess(LogSegmentMetadata segment) {
                        removeCachedLogWriter();
                        complete(null);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        LOG.error("Completing Log segments encountered exception", cause);
                        complete(cause);
                    }

                    private void complete(final Throwable cause) {
                        closeNoThrow().ensure(new AbstractFunction0<BoxedUnit>() {
                            @Override
                            public BoxedUnit apply() {
                                if (null != cause && shouldThrow) {
                                    FutureUtils.setException(completePromise, cause);
                                } else {
                                    FutureUtils.setValue(completePromise, null);
                                }
                                return BoxedUnit.UNIT;
                            }
                        });
                    }
                });
    }

    @VisibleForTesting
    void closeAndComplete() throws IOException {
        FutureUtils.result(asyncCloseAndComplete(true));
    }

    protected Future<Void> asyncCloseAndComplete() {
        return asyncCloseAndComplete(true);
    }

    @Override
    public void close() throws IOException {
        FutureUtils.result(asyncClose());
    }

    @Override
    public Future<Void> asyncClose() {
        return asyncCloseAndComplete(false);
    }

    /**
     * Close the writer and release all the underlying resources
     */
    protected Future<Void> closeNoThrow() {
        Promise<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new Promise<Void>();
        }
        cancelTruncation();
        Utils.closeSequence(bkDistributedLogManager.getScheduler(),
                true, /** ignore close errors **/
                getCachedLogWriter(),
                getAllocatedLogWriter(),
                getCachedWriteHandler()
        ).proxyTo(closeFuture);
        return closeFuture;
    }

    @Override
    public void abort() throws IOException {
        FutureUtils.result(asyncAbort());
    }

    @Override
    public Future<Void> asyncAbort() {
        Promise<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new Promise<Void>();
        }
        cancelTruncation();
        Abortables.abortSequence(bkDistributedLogManager.getScheduler(),
                getCachedLogWriter(),
                getAllocatedLogWriter(),
                getCachedWriteHandler()).proxyTo(closeFuture);
        return closeFuture;
    }

    // used by sync writer
    protected BKLogSegmentWriter getLedgerWriter(final long startTxId,
                                                 final boolean allowMaxTxID)
            throws IOException {
        Future<BKLogSegmentWriter> logSegmentWriterFuture = asyncGetLedgerWriter(true);
        BKLogSegmentWriter logSegmentWriter = null;
        if (null != logSegmentWriterFuture) {
            logSegmentWriter = FutureUtils.result(logSegmentWriterFuture);
        }
        if (null == logSegmentWriter || (shouldStartNewSegment(logSegmentWriter) || forceRolling)) {
            logSegmentWriter = FutureUtils.result(rollLogSegmentIfNecessary(
                    logSegmentWriter, startTxId, true /* bestEffort */, allowMaxTxID));
        }
        return logSegmentWriter;
    }

    // used by async writer
    synchronized protected Future<BKLogSegmentWriter> asyncGetLedgerWriter(boolean resetOnError) {
        final BKLogSegmentWriter ledgerWriter = getCachedLogWriter();
        Future<BKLogSegmentWriter> ledgerWriterFuture = getCachedLogWriterFuture();
        if (null == ledgerWriterFuture || null == ledgerWriter) {
            return null;
        }

        // Handle the case where the last call to write actually caused an error in the log
        if ((ledgerWriter.isLogSegmentInError() || forceRecovery) && resetOnError) {
            // Close the ledger writer so that we will recover and start a new log segment
            Future<Void> closeFuture;
            if (ledgerWriter.isLogSegmentInError()) {
                closeFuture = ledgerWriter.asyncAbort();
            } else {
                closeFuture = ledgerWriter.asyncClose();
            }
            return closeFuture.flatMap(
                    new AbstractFunction1<Void, Future<BKLogSegmentWriter>>() {
                @Override
                public Future<BKLogSegmentWriter> apply(Void result) {
                    removeCachedLogWriter();

                    if (ledgerWriter.isLogSegmentInError()) {
                        return Future.value(null);
                    }

                    BKLogWriteHandler writeHandler;
                    try {
                        writeHandler = getWriteHandler();
                    } catch (IOException e) {
                        return Future.exception(e);
                    }
                    if (null != writeHandler && forceRecovery) {
                        return writeHandler.completeAndCloseLogSegment(ledgerWriter)
                                .map(new AbstractFunction1<LogSegmentMetadata, BKLogSegmentWriter>() {
                            @Override
                            public BKLogSegmentWriter apply(LogSegmentMetadata completedLogSegment) {
                                return null;
                            }
                        });
                    } else {
                        return Future.value(null);
                    }
                }
            });
        } else {
            return ledgerWriterFuture;
        }
    }

    boolean shouldStartNewSegment(BKLogSegmentWriter ledgerWriter) throws IOException {
        BKLogWriteHandler writeHandler = getWriteHandler();
        return null == ledgerWriter || writeHandler.shouldStartNewSegment(ledgerWriter) || forceRolling;
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

    private Future<BKLogSegmentWriter> asyncStartNewLogSegment(final BKLogWriteHandler writeHandler,
                                                               final long startTxId,
                                                               final boolean allowMaxTxID) {
        return writeHandler.recoverIncompleteLogSegments()
                .flatMap(new AbstractFunction1<Long, Future<BKLogSegmentWriter>>() {
            @Override
            public Future<BKLogSegmentWriter> apply(Long lastTxId) {
                return writeHandler.asyncStartLogSegment(startTxId, false, allowMaxTxID)
                        .onSuccess(new AbstractFunction1<BKLogSegmentWriter, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(BKLogSegmentWriter newSegmentWriter) {
                        cacheLogWriter(newSegmentWriter);
                        return BoxedUnit.UNIT;
                    }
                });
            }
        });
    }

    private Future<BKLogSegmentWriter> closeOldLogSegmentAndStartNewOneWithPermit(
            final BKLogSegmentWriter oldSegmentWriter,
            final BKLogWriteHandler writeHandler,
            final long startTxId,
            final boolean bestEffort,
            final boolean allowMaxTxID) {
        final PermitManager.Permit switchPermit = bkDistributedLogManager.getLogSegmentRollingPermitManager().acquirePermit();
        if (switchPermit.isAllowed()) {
            return closeOldLogSegmentAndStartNewOne(
                    oldSegmentWriter,
                    writeHandler,
                    startTxId,
                    bestEffort,
                    allowMaxTxID
            ).rescue(new Function<Throwable, Future<BKLogSegmentWriter>>() {
                @Override
                public Future<BKLogSegmentWriter> apply(Throwable cause) {
                    if (cause instanceof LockingException) {
                        LOG.warn("We lost lock during completeAndClose log segment for {}. Disable ledger rolling until it is recovered : ",
                                writeHandler.getFullyQualifiedName(), cause);
                        bkDistributedLogManager.getLogSegmentRollingPermitManager().disallowObtainPermits(switchPermit);
                        return Future.value(oldSegmentWriter);
                    } else if (cause instanceof ZKException) {
                        ZKException zke = (ZKException) cause;
                        if (ZKException.isRetryableZKException(zke)) {
                            LOG.warn("Encountered zookeeper connection issues during completeAndClose log segment for {}." +
                                    " Disable ledger rolling until it is recovered : {}", writeHandler.getFullyQualifiedName(),
                                    zke.getKeeperExceptionCode());
                            bkDistributedLogManager.getLogSegmentRollingPermitManager().disallowObtainPermits(switchPermit);
                            return Future.value(oldSegmentWriter);
                        }
                    }
                    return Future.exception(cause);
                }
            }).ensure(new AbstractFunction0<BoxedUnit>() {
                @Override
                public BoxedUnit apply() {
                    bkDistributedLogManager.getLogSegmentRollingPermitManager()
                            .releasePermit(switchPermit);
                    return BoxedUnit.UNIT;
                }
            });
        } else {
            bkDistributedLogManager.getLogSegmentRollingPermitManager().releasePermit(switchPermit);
            return Future.value(oldSegmentWriter);
        }
    }

    private Future<BKLogSegmentWriter> closeOldLogSegmentAndStartNewOne(
            final BKLogSegmentWriter oldSegmentWriter,
            final BKLogWriteHandler writeHandler,
            final long startTxId,
            final boolean bestEffort,
            final boolean allowMaxTxID) {
        // we switch only when we could allocate a new log segment.
        BKLogSegmentWriter newSegmentWriter = getAllocatedLogWriter();
        if (null == newSegmentWriter) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Allocating a new log segment from {} for {}.", startTxId,
                        writeHandler.getFullyQualifiedName());
            }
            return writeHandler.asyncStartLogSegment(startTxId, bestEffort, allowMaxTxID)
                    .flatMap(new AbstractFunction1<BKLogSegmentWriter, Future<BKLogSegmentWriter>>() {
                        @Override
                        public Future<BKLogSegmentWriter> apply(BKLogSegmentWriter newSegmentWriter) {
                            if (null == newSegmentWriter) {
                                if (bestEffort) {
                                    return Future.value(oldSegmentWriter);
                                } else {
                                    return Future.exception(
                                            new UnexpectedException("StartLogSegment returns null for bestEffort rolling"));
                                }
                            }
                            cacheAllocatedLogWriter(newSegmentWriter);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Allocated a new log segment from {} for {}.", startTxId,
                                        writeHandler.getFullyQualifiedName());
                            }
                            return completeOldSegmentAndCacheNewLogSegmentWriter(oldSegmentWriter, newSegmentWriter);
                        }
                    });
        } else {
            return completeOldSegmentAndCacheNewLogSegmentWriter(oldSegmentWriter, newSegmentWriter);
        }
    }

    private Future<BKLogSegmentWriter> completeOldSegmentAndCacheNewLogSegmentWriter(
            BKLogSegmentWriter oldSegmentWriter,
            final BKLogSegmentWriter newSegmentWriter) {
        final Promise<BKLogSegmentWriter> completePromise = new Promise<BKLogSegmentWriter>();
        // complete the old log segment
        writeHandler.completeAndCloseLogSegment(oldSegmentWriter)
                .addEventListener(new FutureEventListener<LogSegmentMetadata>() {

                    @Override
                    public void onSuccess(LogSegmentMetadata value) {
                        cacheLogWriter(newSegmentWriter);
                        removeAllocatedLogWriter();
                        FutureUtils.setValue(completePromise, newSegmentWriter);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        FutureUtils.setException(completePromise, cause);
                    }
                });
        return completePromise;
    }

    synchronized protected Future<BKLogSegmentWriter> rollLogSegmentIfNecessary(
            final BKLogSegmentWriter segmentWriter,
            long startTxId,
            boolean bestEffort,
            boolean allowMaxTxID) {
        final BKLogWriteHandler writeHandler;
        try {
            writeHandler = getWriteHandler();
        } catch (IOException e) {
            return Future.exception(e);
        }
        Future<BKLogSegmentWriter> rollPromise;
        if (null != segmentWriter && (writeHandler.shouldStartNewSegment(segmentWriter) || forceRolling)) {
            rollPromise = closeOldLogSegmentAndStartNewOneWithPermit(
                    segmentWriter, writeHandler, startTxId, bestEffort, allowMaxTxID);
        } else if (null == segmentWriter) {
            rollPromise = asyncStartNewLogSegment(writeHandler, startTxId, allowMaxTxID);
        } else {
            rollPromise = Future.value(segmentWriter);
        }
        return rollPromise.map(new AbstractFunction1<BKLogSegmentWriter, BKLogSegmentWriter>() {
            @Override
            public BKLogSegmentWriter apply(BKLogSegmentWriter newSegmentWriter) {
                if (segmentWriter == newSegmentWriter) {
                    return newSegmentWriter;
                }
                truncateLogSegmentsIfNecessary(writeHandler);
                return newSegmentWriter;
            }
        });
    }

    protected synchronized void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (null != closePromise) {
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
