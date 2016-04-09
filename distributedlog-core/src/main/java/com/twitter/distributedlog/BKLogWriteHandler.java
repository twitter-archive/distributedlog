package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.function.GetLastTxIdFunction;
import com.twitter.distributedlog.impl.BKLogSegmentEntryWriter;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForWriter;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.logsegment.RollingPolicy;
import com.twitter.distributedlog.logsegment.SizeBasedRollingPolicy;
import com.twitter.distributedlog.logsegment.TimeBasedRollingPolicy;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.FutureUtils.FutureEventListenerRunnable;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.Utils;
import com.twitter.distributedlog.zk.ZKOp;
import com.twitter.distributedlog.zk.ZKTransaction;
import com.twitter.distributedlog.zk.ZKVersionedSetOp;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.impl.ZKLogSegmentFilters.WRITE_HANDLE_FILTER;

/**
 * Log Handler for Writers.
 *
 * <h3>Metrics</h3>
 * All the metrics about log write handler are exposed under scope `segments`.
 * <ul>
 * <li> `segments`/open : opstats. latency characteristics on starting a new log segment.
 * <li> `segments`/close : opstats. latency characteristics on completing an inprogress log segment.
 * <li> `segments`/recover : opstats. latency characteristics on recovering a log segment.
 * <li> `segments`/delete : opstats. latency characteristics on deleting a log segment.
 * </ul>
 */
class BKLogWriteHandler extends BKLogHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogReadHandler.class);

    protected final DistributedLock lock;
    protected final int ensembleSize;
    protected final int writeQuorumSize;
    protected final int ackQuorumSize;
    protected final LedgerAllocator ledgerAllocator;
    protected final MaxTxId maxTxId;
    protected final MaxLogSegmentSequenceNo maxLogSegmentSequenceNo;
    protected final boolean sanityCheckTxnId;
    protected final boolean validateLogSegmentSequenceNumber;
    protected final int regionId;
    protected volatile boolean closed = false;
    protected final RollingPolicy rollingPolicy;
    protected Future<DistributedLock> lockFuture = null;
    protected final PermitLimiter writeLimiter;
    protected final FeatureProvider featureProvider;
    protected final DynamicDistributedLogConfiguration dynConf;
    protected final MetadataUpdater metadataUpdater;
    // tracking the inprogress log segments
    protected final LinkedList<Long> inprogressLSSNs;

    // Recover Functions
    private final RecoverLogSegmentFunction recoverLogSegmentFunction =
            new RecoverLogSegmentFunction();
    private final AbstractFunction1<List<LogSegmentMetadata>, Future<Long>> recoverLogSegmentsFunction =
            new AbstractFunction1<List<LogSegmentMetadata>, Future<Long>>() {
                @Override
                public Future<Long> apply(List<LogSegmentMetadata> segmentList) {
                    LOG.info("Initiating Recovery For {} : {}", getFullyQualifiedName(), segmentList);
                    // if lastLedgerRollingTimeMillis is not updated, we set it to now.
                    synchronized (BKLogWriteHandler.this) {
                        if (lastLedgerRollingTimeMillis < 0) {
                            lastLedgerRollingTimeMillis = Utils.nowInMillis();
                        }
                    }

                    if (validateLogSegmentSequenceNumber) {
                        synchronized (inprogressLSSNs) {
                            for (LogSegmentMetadata segment : segmentList) {
                                if (segment.isInProgress()) {
                                    inprogressLSSNs.addLast(segment.getLogSegmentSequenceNumber());
                                }
                            }
                        }
                    }

                    return FutureUtils.processList(segmentList, recoverLogSegmentFunction, scheduler).map(
                            GetLastTxIdFunction.INSTANCE);
                }
            };

    // Stats
    private final StatsLogger perLogStatsLogger;
    private final OpStatsLogger closeOpStats;
    private final OpStatsLogger openOpStats;
    private final OpStatsLogger recoverOpStats;
    private final OpStatsLogger deleteOpStats;

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogWriteHandler(ZKLogMetadataForWriter logMetadata,
                      DistributedLogConfiguration conf,
                      ZooKeeperClientBuilder zkcBuilder,
                      BookKeeperClientBuilder bkcBuilder,
                      LogSegmentMetadataStore metadataStore,
                      OrderedScheduler scheduler,
                      LedgerAllocator allocator,
                      StatsLogger statsLogger,
                      StatsLogger perLogStatsLogger,
                      AlertStatsLogger alertStatsLogger,
                      String clientId,
                      int regionId,
                      PermitLimiter writeLimiter,
                      FeatureProvider featureProvider,
                      DynamicDistributedLogConfiguration dynConf,
                      DistributedLock lock /** owned by handler **/) {
        super(logMetadata, conf, zkcBuilder, bkcBuilder, metadataStore,
              scheduler, statsLogger, alertStatsLogger, null, WRITE_HANDLE_FILTER, clientId);
        this.perLogStatsLogger = perLogStatsLogger;
        this.writeLimiter = writeLimiter;
        this.featureProvider = featureProvider;
        this.dynConf = dynConf;
        this.ledgerAllocator = allocator;
        this.lock = lock;
        this.metadataUpdater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(conf, metadataStore);

        ensembleSize = conf.getEnsembleSize();

        if (ensembleSize < conf.getWriteQuorumSize()) {
            writeQuorumSize = ensembleSize;
            LOG.warn("Setting write quorum size {} greater than ensemble size {}",
                conf.getWriteQuorumSize(), ensembleSize);
        } else {
            writeQuorumSize = conf.getWriteQuorumSize();
        }
        if (writeQuorumSize < conf.getAckQuorumSize()) {
            ackQuorumSize = writeQuorumSize;
            LOG.warn("Setting write ack quorum size {} greater than write quorum size {}",
                conf.getAckQuorumSize(), writeQuorumSize);
        } else {
            ackQuorumSize = conf.getAckQuorumSize();
        }

        if (conf.getEncodeRegionIDInLogSegmentMetadata()) {
            this.regionId = regionId;
        } else {
            this.regionId = DistributedLogConstants.LOCAL_REGION_ID;
        }
        this.sanityCheckTxnId = conf.getSanityCheckTxnID();
        this.validateLogSegmentSequenceNumber = conf.isLogSegmentSequenceNumberValidationEnabled();

        // Construct the max sequence no
        maxLogSegmentSequenceNo = new MaxLogSegmentSequenceNo(logMetadata.getMaxLSSNData());
        inprogressLSSNs = new LinkedList<Long>();
        // Construct the max txn id.
        maxTxId = new MaxTxId(zooKeeperClient, logMetadata.getMaxTxIdPath(),
                conf.getSanityCheckTxnID(), logMetadata.getMaxTxIdData());

        // Schedule fetching ledgers list in background before we access it.
        // We don't need to watch the ledgers list changes for writer, as it manages ledgers list.
        scheduleGetLedgersTask(false, true);

        // Initialize other parameters.
        setLastLedgerRollingTimeMillis(Utils.nowInMillis());

        // Rolling Policy
        if (conf.getLogSegmentRollingIntervalMinutes() > 0) {
            rollingPolicy = new TimeBasedRollingPolicy(conf.getLogSegmentRollingIntervalMinutes() * 60 * 1000);
        } else {
            rollingPolicy = new SizeBasedRollingPolicy(conf.getMaxLogSegmentBytes());
        }

        // Stats
        StatsLogger segmentsStatsLogger = statsLogger.scope("segments");
        openOpStats = segmentsStatsLogger.getOpStatsLogger("open");
        closeOpStats = segmentsStatsLogger.getOpStatsLogger("close");
        recoverOpStats = segmentsStatsLogger.getOpStatsLogger("recover");
        deleteOpStats = segmentsStatsLogger.getOpStatsLogger("delete");
    }

    // Transactional operations for MaxLogSegmentSequenceNo
    void storeMaxSequenceNumber(final Transaction txn,
                                final MaxLogSegmentSequenceNo maxSeqNo,
                                final long seqNo,
                                final boolean isInprogress) {
        byte[] data = DLUtils.serializeLogSegmentSequenceNumber(seqNo);
        Op zkOp = Op.setData(logMetadata.getLogSegmentsPath(), data, maxSeqNo.getZkVersion());
        txn.addOp(new ZKVersionedSetOp(zkOp, new Transaction.OpListener<Version>() {
            @Override
            public void onCommit(Version version) {
                if (validateLogSegmentSequenceNumber) {
                    synchronized (inprogressLSSNs) {
                        if (isInprogress) {
                            inprogressLSSNs.add(seqNo);
                        } else {
                            inprogressLSSNs.removeFirst();
                        }
                    }
                }
                maxSeqNo.update((ZkVersion) version, seqNo);
            }

            @Override
            public void onAbort(Throwable t) {
                // no-op
            }
        }));
    }

    // Transactional operations for MaxTxId
    void storeMaxTxId(final ZKTransaction txn,
                      final MaxTxId maxTxId,
                      final long txId) {
        byte[] data = maxTxId.couldStore(txId);
        if (null != data) {
            Op zkOp = Op.setData(maxTxId.getZkPath(), data, -1);
            txn.addOp(new ZKVersionedSetOp(zkOp, new Transaction.OpListener<Version>() {
                @Override
                public void onCommit(Version version) {
                    maxTxId.setMaxTxId(txId);
                }

                @Override
                public void onAbort(Throwable t) {

                }
            }));
        }
    }

    // Transactional operations for logsegment
    void writeLogSegment(final ZKTransaction txn,
                         final List<ACL> acl,
                         final String inprogressSegmentName,
                         final LogSegmentMetadata metadata,
                         final String path) {
        byte[] finalisedData = metadata.getFinalisedData().getBytes(UTF_8);
        Op zkOp = Op.create(path, finalisedData, acl, CreateMode.PERSISTENT);
        txn.addOp(new ZKOp(zkOp) {
            @Override
            protected void commitOpResult(OpResult opResult) {
                addLogSegmentToCache(inprogressSegmentName, metadata);
            }

            @Override
            protected void abortOpResult(Throwable t, OpResult opResult) {
                // no-op
            }
        });
    }

    void deleteLogSegment(final ZKTransaction txn,
                          final String logSegmentName,
                          final String logSegmentPath) {
        Op zkOp = Op.delete(logSegmentPath, -1);
        txn.addOp(new ZKOp(zkOp) {
            @Override
            protected void commitOpResult(OpResult opResult) {
                removeLogSegmentFromCache(logSegmentName);
            }
            @Override
            protected void abortOpResult(Throwable t, OpResult opResult) {
                // no-op
            }
        });
    }

    /**
     * The caller could call this before any actions, which to hold the lock for
     * the write handler of its whole lifecycle. The lock will only be released
     * when closing the write handler.
     *
     * This method is useful to prevent releasing underlying zookeeper lock during
     * recovering/completing log segments. Releasing underlying zookeeper lock means
     * 1) increase latency when re-lock on starting new log segment. 2) increase the
     * possibility of a stream being re-acquired by other instances.
     *
     * @return future represents the lock result
     */
    Future<DistributedLock> lockHandler() {
        if (null != lockFuture) {
            return lockFuture;
        }
        lockFuture = lock.asyncAcquire();
        return lockFuture;
    }

    Future<Void> unlockHandler() {
        if (null != lockFuture) {
            return lock.asyncClose();
        } else {
            return Future.Void();
        }
    }

    void register(Watcher watcher) {
        this.zooKeeperClient.register(watcher);
    }

    /**
     * Start a new log segment in a BookKeeper ledger.
     * First ensure that we have the write lock for this journal.
     * Then create a ledger and stream based on that ledger.
     * The ledger id is written to the inprogress znode, so that in the
     * case of a crash, a recovery process can find the ledger we were writing
     * to when we crashed.
     *
     * @param txId First transaction id to be written to the stream
     * @return
     * @throws IOException
     */
    public BKLogSegmentWriter startLogSegment(long txId) throws IOException {
        return startLogSegment(txId, false, false);
    }

    /**
     * Start a new log segment in a BookKeeper ledger.
     * First ensure that we have the write lock for this journal.
     * Then create a ledger and stream based on that ledger.
     * The ledger id is written to the inprogress znode, so that in the
     * case of a crash, a recovery process can find the ledger we were writing
     * to when we crashed.
     *
     * @param txId First transaction id to be written to the stream
     * @param bestEffort
     * @param allowMaxTxID
     *          allow using max tx id to start log segment
     * @return
     * @throws IOException
     */
    public BKLogSegmentWriter startLogSegment(long txId, boolean bestEffort, boolean allowMaxTxID)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            BKLogSegmentWriter writer = doStartLogSegment(txId, bestEffort, allowMaxTxID);
            success = true;
            return writer;
        } finally {
            if (success) {
                openOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                openOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    protected long assignLogSegmentSequenceNumber() throws IOException {
        // For any active stream we will always make sure that there is at least one
        // active ledger (except when the stream first starts out). Therefore when we
        // see no ledger metadata for a stream, we assume that this is the first ledger
        // in the stream
        long logSegmentSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
        boolean logSegmentsFound = false;

        if (LogSegmentMetadata.supportsLogSegmentSequenceNo(conf.getDLLedgerMetadataLayoutVersion())) {
            List<LogSegmentMetadata> ledgerListDesc = getFilteredLedgerListDesc(false, false);
            Long nextLogSegmentSeqNo = DLUtils.nextLogSegmentSequenceNumber(ledgerListDesc);

            if (null == nextLogSegmentSeqNo) {
                logSegmentsFound = false;
                // we don't find last assigned log segment sequence number
                // then we start the log segment with configured FirstLogSegmentSequenceNumber.
                logSegmentSeqNo = conf.getFirstLogSegmentSequenceNumber();
            } else {
                logSegmentsFound = true;
                // latest log segment is assigned with a sequence number, start with next sequence number
                logSegmentSeqNo = nextLogSegmentSeqNo;
            }
        }

        // We only skip log segment sequence number validation only when no log segments found &
        // the maximum log segment sequence number is "UNASSIGNED".
        if (!logSegmentsFound &&
            (DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO == maxLogSegmentSequenceNo.getSequenceNumber())) {
            // no ledger seqno stored in /ledgers before
            LOG.info("No max ledger sequence number found while creating log segment {} for {}.",
                logSegmentSeqNo, getFullyQualifiedName());
        } else if (maxLogSegmentSequenceNo.getSequenceNumber() + 1 != logSegmentSeqNo) {
            LOG.warn("Unexpected max log segment sequence number {} for {} : list of cached segments = {}",
                new Object[]{maxLogSegmentSequenceNo.getSequenceNumber(), getFullyQualifiedName(),
                    getCachedLogSegments(LogSegmentMetadata.DESC_COMPARATOR)});
            // there is max log segment number recorded there and it isn't match. throw exception.
            throw new DLIllegalStateException("Unexpected max log segment sequence number "
                + maxLogSegmentSequenceNo.getSequenceNumber() + " for " + getFullyQualifiedName()
                + ", expected " + (logSegmentSeqNo - 1));
        }

        return logSegmentSeqNo;
    }

    protected BKLogSegmentWriter doStartLogSegment(long txId, boolean bestEffort, boolean allowMaxTxID) throws IOException {
        return FutureUtils.result(asyncStartLogSegment(txId, bestEffort, allowMaxTxID));
    }

    protected Future<BKLogSegmentWriter> asyncStartLogSegment(long txId,
                                                              boolean bestEffort,
                                                              boolean allowMaxTxID) {
        Promise<BKLogSegmentWriter> promise = new Promise<BKLogSegmentWriter>();
        try {
            lock.checkOwnershipAndReacquire();
        } catch (LockingException e) {
            FutureUtils.setException(promise, e);
            return promise;
        }
        doStartLogSegment(txId, bestEffort, allowMaxTxID, promise);
        return promise;
    }

    protected void doStartLogSegment(final long txId,
                                     final boolean bestEffort,
                                     final boolean allowMaxTxID,
                                     final Promise<BKLogSegmentWriter> promise) {
        // validate the tx id
        if ((txId < 0) ||
                (!allowMaxTxID && (txId == DistributedLogConstants.MAX_TXID))) {
            FutureUtils.setException(promise, new IOException("Invalid Transaction Id " + txId));
            return;
        }
        if (this.sanityCheckTxnId) {
            long highestTxIdWritten = maxTxId.get();
            if (txId < highestTxIdWritten) {
                if (highestTxIdWritten == DistributedLogConstants.MAX_TXID) {
                    LOG.error("We've already marked the stream as ended and attempting to start a new log segment");
                    FutureUtils.setException(promise, new EndOfStreamException("Writing to a stream after it has been marked as completed"));
                    return;
                }
                else {
                    LOG.error("We've already seen TxId {} the max TXId is {}", txId, highestTxIdWritten);
                    FutureUtils.setException(promise, new TransactionIdOutOfOrderException(txId, highestTxIdWritten));
                    return;
                }
            }
        }

        try {
            ledgerAllocator.allocate();
        } catch (IOException e) {
            // failed to issue an allocation request
            failStartLogSegment(promise, bestEffort, e);
            return;
        }

        // start the transaction from zookeeper
        final ZKTransaction txn = new ZKTransaction(zooKeeperClient);

        // failpoint injected before creating ledger
        try {
            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate);
        } catch (IOException ioe) {
            failStartLogSegment(promise, bestEffort, ioe);
            return;
        }

        ledgerAllocator.tryObtain(txn, new Transaction.OpListener<LedgerHandle>() {
            @Override
            public void onCommit(LedgerHandle lh) {
                // no-op
            }

            @Override
            public void onAbort(Throwable t) {
                // no-op
            }
        }).addEventListener(new FutureEventListener<LedgerHandle>() {

            @Override
            public void onSuccess(LedgerHandle lh) {
                // try-obtain succeed
                createInprogressLogSegment(
                        txn,
                        txId,
                        lh,
                        bestEffort,
                        promise);
            }

            @Override
            public void onFailure(Throwable cause) {
                failStartLogSegment(promise, bestEffort, cause);
            }
        });
    }

    private void failStartLogSegment(Promise<BKLogSegmentWriter> promise,
                                     boolean bestEffort,
                                     Throwable cause) {
        if (bestEffort) {
            FutureUtils.setValue(promise, null);
        } else {
            FutureUtils.setException(promise, cause);
        }
    }

    // once the ledger handle is obtained from allocator, this function should guarantee
    // either the transaction is executed or aborted. Otherwise, the ledger handle will
    // just leak from the allocation pool - hence cause "No Ledger Allocator"
    private void createInprogressLogSegment(ZKTransaction txn,
                                            final long txId,
                                            final LedgerHandle lh,
                                            boolean bestEffort,
                                            final Promise<BKLogSegmentWriter> promise) {
        final long logSegmentSeqNo;
        try {
            FailpointUtils.checkFailPoint(
                    FailpointUtils.FailPointName.FP_StartLogSegmentOnAssignLogSegmentSequenceNumber);
            logSegmentSeqNo = assignLogSegmentSequenceNumber();
        } catch (IOException e) {
            // abort the current prepared transaction
            txn.abort(e);
            failStartLogSegment(promise, bestEffort, e);
            return;
        }

        final String inprogressZnodeName = inprogressZNodeName(lh.getId(), txId, logSegmentSeqNo);
        final String inprogressZnodePath = inprogressZNode(lh.getId(), txId, logSegmentSeqNo);
        final LogSegmentMetadata l =
            new LogSegmentMetadata.LogSegmentMetadataBuilder(inprogressZnodePath,
                conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), txId)
                    .setLogSegmentSequenceNo(logSegmentSeqNo)
                    .setRegionId(regionId)
                    .setEnvelopeEntries(LogSegmentMetadata.supportsEnvelopedEntries(conf.getDLLedgerMetadataLayoutVersion()))
                    .build();

        // Create an inprogress segment
        writeLogSegment(
                txn,
                zooKeeperClient.getDefaultACL(),
                inprogressZnodeName,
                l,
                inprogressZnodePath);

        // Try storing max sequence number.
        LOG.debug("Try storing max sequence number in startLogSegment {} : {}", inprogressZnodePath, logSegmentSeqNo);
        storeMaxSequenceNumber(txn, maxLogSegmentSequenceNo, logSegmentSeqNo, true);

        // Try storing max tx id.
        LOG.debug("Try storing MaxTxId in startLogSegment  {} {}", inprogressZnodePath, txId);
        storeMaxTxId(txn, maxTxId, txId);

        txn.execute().addEventListener(FutureEventListenerRunnable.of(new FutureEventListener<Void>() {

            @Override
            public void onSuccess(Void value) {
                try {
                    FutureUtils.setValue(promise, new BKLogSegmentWriter(
                            getFullyQualifiedName(),
                            inprogressZnodeName,
                            conf,
                            conf.getDLLedgerMetadataLayoutVersion(),
                            new BKLogSegmentEntryWriter(lh),
                            lock,
                            txId,
                            logSegmentSeqNo,
                            scheduler,
                            statsLogger,
                            perLogStatsLogger,
                            alertStatsLogger,
                            writeLimiter,
                            featureProvider,
                            dynConf));
                } catch (IOException ioe) {
                    failStartLogSegment(promise, false, ioe);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                failStartLogSegment(promise, false, cause);
            }
        }, scheduler));
    }

    boolean shouldStartNewSegment(BKLogSegmentWriter writer) {
        return rollingPolicy.shouldRollover(writer, lastLedgerRollingTimeMillis);
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     * <p/>
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    Future<LogSegmentMetadata> completeAndCloseLogSegment(final BKLogSegmentWriter writer) {
        final Promise<LogSegmentMetadata> promise = new Promise<LogSegmentMetadata>();
        completeAndCloseLogSegment(writer, promise);
        return promise;
    }

    private void completeAndCloseLogSegment(final BKLogSegmentWriter writer,
                                            final Promise<LogSegmentMetadata> promise) {
        writer.asyncClose().addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                // in theory closeToFinalize should throw exception if a stream is in error.
                // just in case, add another checking here to make sure we don't close log segment is a stream is in error.
                if (writer.shouldFailCompleteLogSegment()) {
                    FutureUtils.setException(promise,
                            new IOException("LogSegmentWriter for " + writer.getFullyQualifiedLogSegment() + " is already in error."));
                    return;
                }
                doCompleteAndCloseLogSegment(
                        inprogressZNodeName(writer.getLogSegmentId(), writer.getStartTxId(), writer.getLogSegmentSequenceNumber()),
                        writer.getLogSegmentSequenceNumber(),
                        writer.getLogSegmentId(),
                        writer.getStartTxId(),
                        writer.getLastTxId(),
                        writer.getPositionWithinLogSegment(),
                        writer.getLastDLSN().getEntryId(),
                        writer.getLastDLSN().getSlotId(),
                        promise);
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.setException(promise, cause);
            }
        });
    }

    @VisibleForTesting
    LogSegmentMetadata completeAndCloseLogSegment(long logSegmentSeqNo,
                                                  long ledgerId,
                                                  long firstTxId,
                                                  long lastTxId,
                                                  int recordCount)
        throws IOException {
        return completeAndCloseLogSegment(inprogressZNodeName(ledgerId, firstTxId, logSegmentSeqNo), logSegmentSeqNo,
            ledgerId, firstTxId, lastTxId, recordCount, -1, -1);
    }

    /**
     * Finalize a log segment. If the journal manager is currently
     * writing to a ledger, ensure that this is the ledger of the log segment
     * being finalized.
     * <p/>
     * Otherwise this is the recovery case. In the recovery case, ensure that
     * the firstTxId of the ledger matches firstTxId for the segment we are
     * trying to finalize.
     */
    LogSegmentMetadata completeAndCloseLogSegment(String inprogressZnodeName, long logSegmentSeqNo,
                                                  long ledgerId, long firstTxId, long lastTxId,
                                                  int recordCount, long lastEntryId, long lastSlotId)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            LogSegmentMetadata completedLogSegment =
                    doCompleteAndCloseLogSegment(inprogressZnodeName, logSegmentSeqNo,
                            ledgerId, firstTxId, lastTxId, recordCount,
                            lastEntryId, lastSlotId);
            success = true;
            return completedLogSegment;
        } finally {
            if (success) {
                closeOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                closeOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    protected long computeStartSequenceId(LogSegmentMetadata segment) throws IOException {
        if (!segment.isInProgress()) {
            return segment.getStartSequenceId();
        }

        long startSequenceId = DistributedLogConstants.UNASSIGNED_SEQUENCE_ID;

        // we only record sequence id when both write version and logsegment's version support sequence id
        if (LogSegmentMetadata.supportsSequenceId(conf.getDLLedgerMetadataLayoutVersion())
                && segment.supportsSequenceId()) {
            List<LogSegmentMetadata> logSegmentDescList = getFilteredLedgerListDesc(false, false);
            startSequenceId = DLUtils.computeStartSequenceId(logSegmentDescList, segment);
        }

        return startSequenceId;
    }

    /**
     * Close log segment
     *
     * @param inprogressZnodeName
     * @param logSegmentSeqNo
     * @param ledgerId
     * @param firstTxId
     * @param lastTxId
     * @param recordCount
     * @param lastEntryId
     * @param lastSlotId
     * @throws IOException
     */
    protected LogSegmentMetadata doCompleteAndCloseLogSegment(
            String inprogressZnodeName,
            long logSegmentSeqNo,
            long ledgerId,
            long firstTxId,
            long lastTxId,
            int recordCount,
            long lastEntryId,
            long lastSlotId) throws IOException {
        Promise<LogSegmentMetadata> promise = new Promise<LogSegmentMetadata>();
        doCompleteAndCloseLogSegment(
                inprogressZnodeName,
                logSegmentSeqNo,
                ledgerId,
                firstTxId,
                lastTxId,
                recordCount,
                lastEntryId,
                lastSlotId,
                promise);
        return FutureUtils.result(promise);
    }

    protected void doCompleteAndCloseLogSegment(final String inprogressZnodeName,
                                                long logSegmentSeqNo,
                                                long ledgerId,
                                                long firstTxId,
                                                long lastTxId,
                                                int recordCount,
                                                long lastEntryId,
                                                long lastSlotId,
                                                final Promise<LogSegmentMetadata> promise) {
        try {
            lock.checkOwnershipAndReacquire();
        } catch (IOException ioe) {
            FutureUtils.setException(promise, ioe);
            return;
        }

        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        final String inprogressZnodePath = inprogressZNode(inprogressZnodeName);
        LogSegmentMetadata inprogressLogSegment = readLogSegmentFromCache(inprogressZnodeName);

        // validate log segment
        if (inprogressLogSegment.getLedgerId() != ledgerId) {
            FutureUtils.setException(promise, new IOException(
                "Active ledger has different ID to inprogress. "
                    + inprogressLogSegment.getLedgerId() + " found, "
                    + ledgerId + " expected"));
            return;
        }
        // validate the transaction id
        if (inprogressLogSegment.getFirstTxId() != firstTxId) {
            FutureUtils.setException(promise, new IOException("Transaction id not as expected, "
                + inprogressLogSegment.getFirstTxId() + " found, " + firstTxId + " expected"));
            return;
        }
        // validate the log sequence number
        if (validateLogSegmentSequenceNumber) {
            synchronized (inprogressLSSNs) {
                if (inprogressLSSNs.isEmpty()) {
                    FutureUtils.setException(promise, new UnexpectedException(
                            "Didn't find matched inprogress log segments when completing inprogress "
                                    + inprogressLogSegment));
                    return;
                }
                long leastInprogressLSSN = inprogressLSSNs.getFirst();
                // the log segment sequence number in metadata {@link inprogressLogSegment.getLogSegmentSequenceNumber()}
                // should be same as the sequence number we are completing (logSegmentSeqNo)
                // and
                // it should also be same as the least inprogress log segment sequence number tracked in {@link inprogressLSSNs}
                if ((inprogressLogSegment.getLogSegmentSequenceNumber() != logSegmentSeqNo) ||
                        (leastInprogressLSSN != logSegmentSeqNo)) {
                    FutureUtils.setException(promise, new UnexpectedException(
                            "Didn't find matched inprogress log segments when completing inprogress "
                                    + inprogressLogSegment));
                    return;
                }
            }
        }

        // store max sequence number.
        long maxSeqNo= Math.max(logSegmentSeqNo, maxLogSegmentSequenceNo.getSequenceNumber());
        if (maxLogSegmentSequenceNo.getSequenceNumber() == logSegmentSeqNo ||
                (maxLogSegmentSequenceNo.getSequenceNumber() == logSegmentSeqNo + 1)) {
            // ignore the case that a new inprogress log segment is pre-allocated
            // before completing current inprogress one
            LOG.info("Try storing max sequence number {} in completing {}.",
                    new Object[] { logSegmentSeqNo, inprogressZnodePath });
        } else {
            LOG.warn("Unexpected max ledger sequence number {} found while completing log segment {} for {}",
                    new Object[] { maxLogSegmentSequenceNo.getSequenceNumber(), logSegmentSeqNo, getFullyQualifiedName() });
            if (validateLogSegmentSequenceNumber) {
                FutureUtils.setException(promise, new DLIllegalStateException("Unexpected max log segment sequence number "
                        + maxLogSegmentSequenceNo.getSequenceNumber() + " for " + getFullyQualifiedName()
                        + ", expected " + (logSegmentSeqNo - 1)));
                return;
            }
        }

        // Prepare the completion
        final String nameForCompletedLedger = completedLedgerZNodeName(firstTxId, lastTxId, logSegmentSeqNo);
        final String pathForCompletedLedger = completedLedgerZNode(firstTxId, lastTxId, logSegmentSeqNo);
        long startSequenceId;
        try {
            startSequenceId = computeStartSequenceId(inprogressLogSegment);
        } catch (IOException ioe) {
            FutureUtils.setException(promise, ioe);
            return;
        }
        // write completed ledger znode
        final LogSegmentMetadata completedLogSegment =
                inprogressLogSegment.completeLogSegment(
                        pathForCompletedLedger,
                        lastTxId,
                        recordCount,
                        lastEntryId,
                        lastSlotId,
                        startSequenceId);
        setLastLedgerRollingTimeMillis(completedLogSegment.getCompletionTime());

        // prepare the transaction
        ZKTransaction txn = new ZKTransaction(zooKeeperClient);

        // create completed log segment
        writeLogSegment(
                txn,
                zooKeeperClient.getDefaultACL(),
                nameForCompletedLedger,
                completedLogSegment,
                pathForCompletedLedger);
        // delete inprogress log segment
        deleteLogSegment(txn, inprogressZnodeName, inprogressZnodePath);
        // store max sequence number
        storeMaxSequenceNumber(txn, maxLogSegmentSequenceNo, maxSeqNo, false);
        // update max txn id.
        LOG.debug("Trying storing LastTxId in Finalize Path {} LastTxId {}", pathForCompletedLedger, lastTxId);
        storeMaxTxId(txn, maxTxId, lastTxId);

        txn.execute().addEventListener(FutureEventListenerRunnable.of(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                LOG.info("Completed {} to {} for {} : {}",
                        new Object[] { inprogressZnodeName, nameForCompletedLedger, getFullyQualifiedName(), completedLogSegment });
                FutureUtils.setValue(promise, completedLogSegment);
            }

            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.setException(promise, cause);
            }
        }, scheduler));
    }

    public Future<Long> recoverIncompleteLogSegments() {
        try {
            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_RecoverIncompleteLogSegments);
        } catch (IOException ioe) {
            return Future.exception(ioe);
        }
        return asyncGetFilteredLedgerList(false, false).flatMap(recoverLogSegmentsFunction);
    }

    class RecoverLogSegmentFunction extends Function<LogSegmentMetadata, Future<LogSegmentMetadata>> {

        @Override
        public Future<LogSegmentMetadata> apply(final LogSegmentMetadata l) {
            if (!l.isInProgress()) {
                return Future.value(l);
            }

            LOG.info("Recovering last record in log segment {} for {}.", l, getFullyQualifiedName());
            return asyncReadLastRecord(l, true, true, true).flatMap(
                    new AbstractFunction1<LogRecordWithDLSN, Future<LogSegmentMetadata>>() {
                        @Override
                        public Future<LogSegmentMetadata> apply(LogRecordWithDLSN lastRecord) {
                            return completeLogSegment(l, lastRecord);
                        }
                    });
        }

        private Future<LogSegmentMetadata> completeLogSegment(LogSegmentMetadata l,
                                                              LogRecordWithDLSN lastRecord) {
            LOG.info("Recovered last record in log segment {} for {}.", l, getFullyQualifiedName());

            long endTxId = DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID;
            int recordCount = 0;
            long lastEntryId = -1;
            long lastSlotId = -1;

            if (null != lastRecord) {
                endTxId = lastRecord.getTransactionId();
                recordCount = lastRecord.getLastPositionWithinLogSegment();
                lastEntryId = lastRecord.getDlsn().getEntryId();
                lastSlotId = lastRecord.getDlsn().getSlotId();
            }

            if (endTxId == DistributedLogConstants.INVALID_TXID) {
                LOG.error("Unrecoverable corruption has occurred in segment "
                    + l.toString() + " at path " + l.getZkPath()
                    + ". Unable to continue recovery.");
                return Future.exception(new IOException("Unrecoverable corruption,"
                    + " please check logs."));
            } else if (endTxId == DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID) {
                // TODO: Empty ledger - Ideally we should just remove it?
                endTxId = l.getFirstTxId();
            }

            Promise<LogSegmentMetadata> promise = new Promise<LogSegmentMetadata>();
            doCompleteAndCloseLogSegment(
                    l.getZNodeName(),
                    l.getLogSegmentSequenceNumber(),
                    l.getLedgerId(),
                    l.getFirstTxId(),
                    endTxId,
                    recordCount,
                    lastEntryId,
                    lastSlotId,
                    promise);
            return promise;
        }

    }

    public void deleteLog() throws IOException {
        lock.checkOwnershipAndReacquire();
        FutureUtils.result(purgeLogSegmentsOlderThanTxnId(-1));

        try {
            Utils.closeQuietly(lock);
            zooKeeperClient.get().exists(logMetadata.getLogSegmentsPath(), false);
            zooKeeperClient.get().exists(logMetadata.getMaxTxIdPath(), false);
            if (logMetadata.getLogRootPath().toLowerCase().contains("distributedlog")) {
                ZKUtil.deleteRecursive(zooKeeperClient.get(), logMetadata.getLogRootPath());
            } else {
                LOG.warn("Skip deletion of unrecognized ZK Path {}", logMetadata.getLogRootPath());
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting log znodes", ie);
            throw new DLInterruptedException("Interrupted while deleting " + logMetadata.getLogRootPath(), ie);
        } catch (KeeperException ke) {
            LOG.error("Error deleting" + logMetadata.getLogRootPath() + " in zookeeper", ke);
        }
    }

    Future<List<LogSegmentMetadata>> setLogSegmentsOlderThanDLSNTruncated(final DLSN dlsn) {
        if (DLSN.InvalidDLSN == dlsn) {
            List<LogSegmentMetadata> emptyList = new ArrayList<LogSegmentMetadata>(0);
            return Future.value(emptyList);
        }
        scheduleGetAllLedgersTaskIfNeeded();
        return asyncGetFullLedgerList(false, false).flatMap(
                new AbstractFunction1<List<LogSegmentMetadata>, Future<List<LogSegmentMetadata>>>() {
                    @Override
                    public Future<List<LogSegmentMetadata>> apply(List<LogSegmentMetadata> logSegments) {
                        return setLogSegmentsOlderThanDLSNTruncated(logSegments, dlsn);
                    }
                });
    }

    private Future<List<LogSegmentMetadata>> setLogSegmentsOlderThanDLSNTruncated(List<LogSegmentMetadata> logSegments,
                                                                                  final DLSN dlsn) {
        LOG.debug("Setting truncation status on logs older than {} from {} for {}",
                new Object[]{dlsn, logSegments, getFullyQualifiedName()});
        List<LogSegmentMetadata> truncateList = new ArrayList<LogSegmentMetadata>(logSegments.size());
        LogSegmentMetadata partialTruncate = null;
        LOG.info("{}: Truncating log segments older than {}", getFullyQualifiedName(), dlsn);
        int numCandidates = getNumCandidateLogSegmentsToTruncate(logSegments);
        for (int i = 0; i < numCandidates; i++) {
            LogSegmentMetadata l = logSegments.get(i);
            if (!l.isInProgress()) {
                if (l.getLastDLSN().compareTo(dlsn) < 0) {
                    LOG.debug("{}: Truncating log segment {} ", getFullyQualifiedName(), l);
                    truncateList.add(l);
                } else if (l.getFirstDLSN().compareTo(dlsn) < 0) {
                    // Can be satisfied by at most one segment
                    if (null != partialTruncate) {
                        String logMsg = String.format("Potential metadata inconsistency for stream %s at segment %s", getFullyQualifiedName(), l);
                        LOG.error(logMsg);
                        return Future.exception(new DLIllegalStateException(logMsg));
                    }
                    LOG.info("{}: Partially truncating log segment {} older than {}.", new Object[] {getFullyQualifiedName(), l, dlsn});
                    partialTruncate = l;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return setLogSegmentTruncationStatus(truncateList, partialTruncate, dlsn);
    }

    private int getNumCandidateLogSegmentsToTruncate(List<LogSegmentMetadata> logSegments) {
        if (logSegments.isEmpty()) {
            return 0;
        } else {
            // we have to keep at least one completed log segment for sequence id
            int numCandidateLogSegments = 0;
            for (LogSegmentMetadata segment : logSegments) {
                if (segment.isInProgress()) {
                    break;
                } else {
                    ++numCandidateLogSegments;
                }
            }

            return numCandidateLogSegments - 1;
        }
    }

    Future<List<LogSegmentMetadata>> purgeLogSegmentsOlderThanTimestamp(final long minTimestampToKeep) {
        if (minTimestampToKeep >= Utils.nowInMillis()) {
            return Future.exception(new IllegalArgumentException(
                    "Invalid timestamp " + minTimestampToKeep + " to purge logs for " + getFullyQualifiedName()));
        }
        return asyncGetFullLedgerList(false, false).flatMap(
                new Function<List<LogSegmentMetadata>, Future<List<LogSegmentMetadata>>>() {
            @Override
            public Future<List<LogSegmentMetadata>> apply(List<LogSegmentMetadata> logSegments) {
                List<LogSegmentMetadata> purgeList = new ArrayList<LogSegmentMetadata>(logSegments.size());

                int numCandidates = getNumCandidateLogSegmentsToTruncate(logSegments);

                for (int iterator = 0; iterator < numCandidates; iterator++) {
                    LogSegmentMetadata l = logSegments.get(iterator);
                    // When application explicitly truncates segments; timestamp based purge is
                    // only used to cleanup log segments that have been marked for truncation
                    if ((l.isTruncated() || !conf.getExplicitTruncationByApplication()) &&
                        !l.isInProgress() && (l.getCompletionTime() < minTimestampToKeep)) {
                        purgeList.add(l);
                    } else {
                        // stop truncating log segments if we find either an inprogress or a partially
                        // truncated log segment
                        break;
                    }
                }
                LOG.info("Deleting log segments older than {} for {} : {}",
                        new Object[] { minTimestampToKeep, getFullyQualifiedName(), purgeList });
                return deleteLogSegments(purgeList);
            }
        });
    }

    Future<List<LogSegmentMetadata>> purgeLogSegmentsOlderThanTxnId(final long minTxIdToKeep) {
        return asyncGetFullLedgerList(true, false).flatMap(
            new AbstractFunction1<List<LogSegmentMetadata>, Future<List<LogSegmentMetadata>>>() {
                @Override
                public Future<List<LogSegmentMetadata>> apply(List<LogSegmentMetadata> logSegments) {
                    int numLogSegmentsToProcess;

                    if (minTxIdToKeep < 0) {
                        // we are deleting the log, we can remove whole log segments
                        numLogSegmentsToProcess = logSegments.size();
                    } else {
                        numLogSegmentsToProcess = getNumCandidateLogSegmentsToTruncate(logSegments);
                    }
                    List<LogSegmentMetadata> purgeList = Lists.newArrayListWithExpectedSize(numLogSegmentsToProcess);
                    for (int iterator = 0; iterator < numLogSegmentsToProcess; iterator++) {
                        LogSegmentMetadata l = logSegments.get(iterator);
                        if ((minTxIdToKeep < 0) ||
                            ((l.isTruncated() || !conf.getExplicitTruncationByApplication()) &&
                            !l.isInProgress() && (l.getLastTxId() < minTxIdToKeep))) {
                            purgeList.add(l);
                        } else {
                            // stop truncating log segments if we find either an inprogress or a partially
                            // truncated log segment
                            break;
                        }
                    }
                    return deleteLogSegments(purgeList);
                }
            });
    }

    private Future<List<LogSegmentMetadata>> setLogSegmentTruncationStatus(
            final List<LogSegmentMetadata> truncateList,
            LogSegmentMetadata partialTruncate,
            DLSN minActiveDLSN) {
        final List<LogSegmentMetadata> listToTruncate = Lists.newArrayListWithCapacity(truncateList.size() + 1);
        final List<LogSegmentMetadata> listAfterTruncated = Lists.newArrayListWithCapacity(truncateList.size() + 1);
        Transaction<Object> updateTxn = metadataUpdater.transaction();
        for(LogSegmentMetadata l : truncateList) {
            if (!l.isTruncated()) {
                LogSegmentMetadata newSegment = metadataUpdater.setLogSegmentTruncated(updateTxn, l);
                listToTruncate.add(l);
                listAfterTruncated.add(newSegment);
            }
        }

        if (null != partialTruncate && (partialTruncate.isNonTruncated() ||
                (partialTruncate.isPartiallyTruncated() && (partialTruncate.getMinActiveDLSN().compareTo(minActiveDLSN) < 0)))) {
            LogSegmentMetadata newSegment = metadataUpdater.setLogSegmentPartiallyTruncated(
                    updateTxn, partialTruncate, minActiveDLSN);
            listToTruncate.add(partialTruncate);
            listAfterTruncated.add(newSegment);
        }

        return updateTxn.execute().map(new AbstractFunction1<Void, List<LogSegmentMetadata>>() {
            @Override
            public List<LogSegmentMetadata> apply(Void value) {
                for (int i = 0; i < listToTruncate.size(); i++) {
                    removeLogSegmentFromCache(listToTruncate.get(i).getSegmentName());
                    LogSegmentMetadata newSegment = listAfterTruncated.get(i);
                    addLogSegmentToCache(newSegment.getSegmentName(), newSegment);
                }
                return listAfterTruncated;
            }
        });
    }

    private Future<List<LogSegmentMetadata>> deleteLogSegments(
            final List<LogSegmentMetadata> logs) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Purging logs for {} : {}", getFullyQualifiedName(), logs);
        }
        return FutureUtils.processList(logs,
                new Function<LogSegmentMetadata, Future<LogSegmentMetadata>>() {
            @Override
            public Future<LogSegmentMetadata> apply(LogSegmentMetadata segment) {
                return deleteLogSegment(segment);
            }
        }, scheduler);
    }

    private Future<LogSegmentMetadata> deleteLogSegment(
            final LogSegmentMetadata ledgerMetadata) {
        LOG.info("Deleting ledger {} for {}", ledgerMetadata, getFullyQualifiedName());
        final Promise<LogSegmentMetadata> promise = new Promise<LogSegmentMetadata>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        promise.addEventListener(new FutureEventListener<LogSegmentMetadata>() {
            @Override
            public void onSuccess(LogSegmentMetadata segment) {
                deleteOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }

            @Override
            public void onFailure(Throwable cause) {
                deleteOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        });
        try {
            bookKeeperClient.get().asyncDeleteLedger(ledgerMetadata.getLedgerId(), new AsyncCallback.DeleteCallback() {
                @Override
                public void deleteComplete(int rc, Object ctx) {
                    if (BKException.Code.NoSuchLedgerExistsException == rc) {
                        LOG.warn("No ledger {} found to delete for {} : {}.",
                                new Object[]{ledgerMetadata.getLedgerId(), getFullyQualifiedName(),
                                        ledgerMetadata});
                    } else if (BKException.Code.OK != rc) {
                        BKException bke = BKException.create(rc);
                        LOG.error("Couldn't delete ledger {} from bookkeeper for {} : ",
                                new Object[]{ledgerMetadata.getLedgerId(), getFullyQualifiedName(), bke});
                        promise.setException(bke);
                        return;
                    }
                    // after the ledger is deleted, we delete the metadata znode
                    scheduler.submit(new Runnable() {
                        @Override
                        public void run() {
                            deleteLogSegmentMetadata(ledgerMetadata, promise);
                        }
                    });
                }
            }, null);
        } catch (IOException e) {
            promise.setException(BKException.create(BKException.Code.BookieHandleNotAvailableException));
        }
        return promise;
    }

    private void deleteLogSegmentMetadata(final LogSegmentMetadata segmentMetadata,
                                          final Promise<LogSegmentMetadata> promise) {
        Transaction<Object> deleteTxn = metadataStore.transaction();
        metadataStore.deleteLogSegment(deleteTxn, segmentMetadata);
        deleteTxn.execute().addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void result) {
                // purge log segment
                removeLogSegmentFromCache(segmentMetadata.getZNodeName());
                promise.setValue(segmentMetadata);
            }

            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof ZKException) {
                    ZKException zke = (ZKException) cause;
                    if (KeeperException.Code.NONODE == zke.getKeeperExceptionCode()) {
                        LOG.error("No log segment {} found for {}.",
                                segmentMetadata, getFullyQualifiedName());
                        // purge log segment
                        removeLogSegmentFromCache(segmentMetadata.getZNodeName());
                        promise.setValue(segmentMetadata);
                        return;
                    }
                }
                LOG.error("Couldn't purge {} for {}: with error {}",
                        new Object[]{ segmentMetadata, getFullyQualifiedName(), cause });
                promise.setException(cause);
            }
        });
    }

    @Override
    public Future<Void> asyncClose() {
        return Utils.closeSequence(scheduler,
                lock,
                ledgerAllocator
        ).flatMap(new AbstractFunction1<Void, Future<Void>>() {
            @Override
            public Future<Void> apply(Void result) {
                return BKLogWriteHandler.super.asyncClose();
            }
        });
    }

    @Override
    public Future<Void> asyncAbort() {
        return asyncClose();
    }

    String completedLedgerZNodeName(long firstTxId, long lastTxId, long logSegmentSeqNo) {
        if (DistributedLogConstants.LOGSEGMENT_NAME_VERSION == conf.getLogSegmentNameVersion()) {
            return String.format("%s_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, logSegmentSeqNo);
        } else {
            return String.format("%s_%018d_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                    firstTxId, lastTxId);
        }
    }

    /**
     * Get the znode path for a finalize ledger
     */
    String completedLedgerZNode(long firstTxId, long lastTxId, long logSegmentSeqNo) {
        return String.format("%s/%s", logMetadata.getLogSegmentsPath(),
                completedLedgerZNodeName(firstTxId, lastTxId, logSegmentSeqNo));
    }

    /**
     * Get the name of the inprogress znode.
     *
     * @return name of the inprogress znode.
     */
    String inprogressZNodeName(long ledgerId, long firstTxId, long logSegmentSeqNo) {
        if (DistributedLogConstants.LOGSEGMENT_NAME_VERSION == conf.getLogSegmentNameVersion()) {
            // Lots of the problems are introduced due to different inprogress names with same ledger sequence number.
            // {@link https://jira.twitter.biz/browse/PUBSUB-1964}
            return String.format("%s_%018d", DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX, logSegmentSeqNo);
        } else {
            return DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX + "_" + Long.toString(firstTxId, 16);
        }
    }

    /**
     * Get the znode path for the inprogressZNode
     */
    String inprogressZNode(long ledgerId, long firstTxId, long logSegmentSeqNo) {
        return logMetadata.getLogSegmentsPath() + "/" + inprogressZNodeName(ledgerId, firstTxId, logSegmentSeqNo);
    }

    String inprogressZNode(String inprogressZNodeName) {
        return logMetadata.getLogSegmentsPath() + "/" + inprogressZNodeName;
    }
}
