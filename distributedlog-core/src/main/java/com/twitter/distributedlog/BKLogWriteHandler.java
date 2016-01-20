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
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForWriter;
import com.twitter.distributedlog.lock.DistributedReentrantLock;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.logsegment.RollingPolicy;
import com.twitter.distributedlog.logsegment.SizeBasedRollingPolicy;
import com.twitter.distributedlog.logsegment.TimeBasedRollingPolicy;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    protected final DistributedReentrantLock lock;
    protected final DistributedReentrantLock deleteLock;
    protected final AtomicInteger startLogSegmentCount = new AtomicInteger(0);
    protected final int ensembleSize;
    protected final int writeQuorumSize;
    protected final int ackQuorumSize;
    protected final LedgerAllocator ledgerAllocator;
    protected final MaxTxId maxTxId;
    protected final MaxLogSegmentSequenceNo maxLogSegmentSequenceNo;
    protected final boolean sanityCheckTxnId;
    protected final int regionId;
    protected FuturePool orderedFuturePool;
    protected final AtomicReference<IOException> metadataException = new AtomicReference<IOException>(null);
    protected volatile boolean closed = false;
    protected final RollingPolicy rollingPolicy;
    protected boolean lockHandler = false;
    protected final PermitLimiter writeLimiter;
    protected final FeatureProvider featureProvider;
    protected final DynamicDistributedLogConfiguration dynConf;
    protected final MetadataUpdater metadataUpdater;

    // Stats
    private final StatsLogger perLogStatsLogger;
    private final OpStatsLogger closeOpStats;
    private final OpStatsLogger openOpStats;
    private final OpStatsLogger recoverOpStats;
    private final OpStatsLogger deleteOpStats;

    abstract class MetadataOp<T> {

        T process() throws IOException {
            if (null != metadataException.get()) {
                throw metadataException.get();
            }
            return processOp();
        }

        abstract T processOp() throws IOException;
    }

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogWriteHandler(ZKLogMetadataForWriter logMetadata,
                      DistributedLogConfiguration conf,
                      ZooKeeperClientBuilder zkcBuilder,
                      BookKeeperClientBuilder bkcBuilder,
                      LogSegmentMetadataStore metadataStore,
                      OrderedScheduler scheduler,
                      FuturePool orderedFuturePool,
                      LedgerAllocator allocator,
                      StatsLogger statsLogger,
                      StatsLogger perLogStatsLogger,
                      AlertStatsLogger alertStatsLogger,
                      String clientId,
                      int regionId,
                      PermitLimiter writeLimiter,
                      FeatureProvider featureProvider,
                      DynamicDistributedLogConfiguration dynConf,
                      DistributedReentrantLock lock, /** owned by handler **/
                      DistributedReentrantLock deleteLock /** owned by handler **/) {
        super(logMetadata, conf, zkcBuilder, bkcBuilder, metadataStore,
              scheduler, statsLogger, alertStatsLogger, null, WRITE_HANDLE_FILTER, clientId);
        this.perLogStatsLogger = perLogStatsLogger;
        this.orderedFuturePool = orderedFuturePool;
        this.writeLimiter = writeLimiter;
        this.featureProvider = featureProvider;
        this.dynConf = dynConf;
        this.ledgerAllocator = allocator;
        this.lock = lock;
        this.deleteLock = deleteLock;
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

        // Construct the max sequence no
        maxLogSegmentSequenceNo = new MaxLogSegmentSequenceNo(logMetadata.getLogSegmentsStat());
        // Construct the max txn id.
        maxTxId = new MaxTxId(zooKeeperClient, logMetadata.getMaxTxIdPath(),
                conf.getSanityCheckTxnID(), logMetadata.getMaxTxIdStat());

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
    void tryStore(org.apache.zookeeper.Transaction txn, MaxLogSegmentSequenceNo maxSeqNo, long seqNo) {
        byte[] data = MaxLogSegmentSequenceNo.toBytes(seqNo);
        txn.setData(logMetadata.getLogSegmentsPath(), data, maxSeqNo.getZkVersion());
    }

    void confirmStore(OpResult result, MaxLogSegmentSequenceNo maxSeqNo, long seqNo) {
        assert (result instanceof OpResult.SetDataResult);
        OpResult.SetDataResult setDataResult = (OpResult.SetDataResult) result;
        maxSeqNo.update(setDataResult.getStat().getVersion(), seqNo);
    }

    void abortStore(MaxLogSegmentSequenceNo maxSeqNo, long seqNo) {
        // nop
    }

    // Transactional operations for MaxTxId
    void tryStore(org.apache.zookeeper.Transaction txn, MaxTxId maxTxId, long txId) throws IOException {
        byte[] data = maxTxId.couldStore(txId);
        if (null != data) {
            txn.setData(maxTxId.getZkPath(), data, -1);
        }
    }

    void confirmStore(OpResult result, MaxTxId maxTxId, long txId) {
        assert (result instanceof OpResult.SetDataResult);
        maxTxId.setMaxTxId(txId);
    }

    void abortStore(MaxTxId maxTxId, long txId) {
        // nop
    }

    // Transactional operations for logsegment
    void tryWrite(org.apache.zookeeper.Transaction txn, List<ACL> acl, LogSegmentMetadata metadata, String path) {
        byte[] finalisedData = metadata.getFinalisedData().getBytes(UTF_8);
        txn.create(path, finalisedData, acl, CreateMode.PERSISTENT);
    }

    void confirmWrite(OpResult result, LogSegmentMetadata metadata, String path) {
        // nop
    }

    void abortWrite(LogSegmentMetadata metadata, String path) {
        // nop
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
     * @return write handler
     * @throws LockingException
     */
    BKLogWriteHandler lockHandler() throws LockingException {
        if (lockHandler) {
            return this;
        }
        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lockHandler = true;
        return this;
    }

    BKLogWriteHandler unlockHandler() throws LockingException {
        if (lockHandler) {
            lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
            lockHandler = false;
        }
        return this;
    }

    void checkMetadataException() throws IOException {
        if (null != metadataException.get()) {
            throw metadataException.get();
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
        boolean ignoreMaxLogSegmentSeqNo = false;

        if (LogSegmentMetadata.supportsLogSegmentSequenceNo(conf.getDLLedgerMetadataLayoutVersion())) {
            List<LogSegmentMetadata> ledgerListDesc = getFilteredLedgerListDesc(false, false);
            Long nextLogSegmentSeqNo = DLUtils.nextLogSegmentSequenceNumber(ledgerListDesc);

            if (null == nextLogSegmentSeqNo) {
                // we don't find last assigned log segment sequence number
                // then we start the log segment with configured FirstLogSegmentSequenceNumber.
                ignoreMaxLogSegmentSeqNo = true;
                logSegmentSeqNo = conf.getFirstLogSegmentSequenceNumber();
            } else {
                // latest log segment is assigned with a sequence number, start with next sequence number
                logSegmentSeqNo = nextLogSegmentSeqNo;
            }
        }

        if (ignoreMaxLogSegmentSeqNo ||
            (DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO == maxLogSegmentSequenceNo.getSequenceNumber())) {
            // no ledger seqno stored in /ledgers before
            LOG.info("No max ledger sequence number found while creating log segment {} for {}.",
                logSegmentSeqNo, getFullyQualifiedName());
        } else if (maxLogSegmentSequenceNo.getSequenceNumber() + 1 != logSegmentSeqNo) {
            LOG.warn("Unexpected max log segment sequence number {} for {} : list of cached segments = {}",
                new Object[]{maxLogSegmentSequenceNo.getSequenceNumber(), getFullyQualifiedName(),
                    getCachedLedgerList(LogSegmentMetadata.DESC_COMPARATOR)});
            // there is max log segment number recorded there and it isn't match. throw exception.
            throw new DLIllegalStateException("Unexpected max log segment sequence number "
                + maxLogSegmentSequenceNo.getSequenceNumber() + " for " + getFullyQualifiedName()
                + ", expected " + (logSegmentSeqNo - 1));
        }

        return logSegmentSeqNo;
    }

    protected BKLogSegmentWriter doStartLogSegment(long txId, boolean bestEffort, boolean allowMaxTxID) throws IOException {
        checkLogExists();

        boolean wroteInprogressZnode = false;

        try {
            if ((txId < 0) ||
                    (!allowMaxTxID && (txId == DistributedLogConstants.MAX_TXID))) {
                throw new IOException("Invalid Transaction Id");
            }

            lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
            startLogSegmentCount.incrementAndGet();

            // sanity check txn id.
            if (this.sanityCheckTxnId) {
                long highestTxIdWritten = maxTxId.get();
                if (txId < highestTxIdWritten) {
                    if (highestTxIdWritten == DistributedLogConstants.MAX_TXID) {
                        LOG.error("We've already marked the stream as ended and attempting to start a new log segment");
                        throw new EndOfStreamException("Writing to a stream after it has been marked as completed");
                    }
                    else {
                        LOG.error("We've already seen TxId {} the max TXId is {}", txId, highestTxIdWritten);
                        throw new TransactionIdOutOfOrderException(txId, highestTxIdWritten);
                    }
                }
            }
            ledgerAllocator.allocate();
            // Obtain a transaction for zookeeper
            org.apache.zookeeper.Transaction txn;
            try {
                txn = zooKeeperClient.get().transaction();
            } catch (InterruptedException e) {
                LOG.error("Interrupted on obtaining a zookeeper transaction for starting log segment on {} : ",
                        getFullyQualifiedName(), e);
                throw new DLInterruptedException("Interrupted on obtaining a zookeeper transaction for starting log segment on "
                        + getFullyQualifiedName(), e);
            }

            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate);

            // Try obtaining an new ledger
            LedgerHandle lh = ledgerAllocator.tryObtain(txn);

            long logSegmentSeqNo = assignLogSegmentSequenceNumber();

            String inprogressZnodeName = inprogressZNodeName(lh.getId(), txId, logSegmentSeqNo);
            String inprogressZnodePath = inprogressZNode(lh.getId(), txId, logSegmentSeqNo);
            LogSegmentMetadata l =
                new LogSegmentMetadata.LogSegmentMetadataBuilder(inprogressZnodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), txId)
                    .setLogSegmentSequenceNo(logSegmentSeqNo)
                    .setRegionId(regionId).build();
            tryWrite(txn, zooKeeperClient.getDefaultACL(), l, inprogressZnodePath);

            // Try storing max sequence number.
            LOG.debug("Try storing max sequence number in startLogSegment {} : {}", inprogressZnodePath, logSegmentSeqNo);
            tryStore(txn, maxLogSegmentSequenceNo, logSegmentSeqNo);

            // Try storing max tx id.
            LOG.debug("Try storing MaxTxId in startLogSegment  {} {}", inprogressZnodePath, txId);
            tryStore(txn, maxTxId, txId);

            // issue transaction
            try {
                List<OpResult> resultList = txn.commit();

                wroteInprogressZnode = true;

                // Allocator handover completed
                {
                    OpResult result = resultList.get(0);
                    ledgerAllocator.confirmObtain(lh, result);
                }
                // Created inprogress log segment completed
                {
                    OpResult result = resultList.get(1);
                    confirmWrite(result, l, inprogressZnodePath);
                    addLogSegmentToCache(inprogressZnodeName, l);
                }
                // Updated max sequence number completed
                {
                    OpResult result = resultList.get(2);
                    confirmStore(result, maxLogSegmentSequenceNo, logSegmentSeqNo);
                }
                // Storing max tx id completed.
                if (resultList.size() == 4) {
                    // get the result of storing max tx id.
                    OpResult result = resultList.get(3);
                    confirmStore(result, maxTxId, txId);
                }
            } catch (InterruptedException ie) {
                // abort ledger allocate
                ledgerAllocator.abortObtain(lh);
                // abort writing inprogress znode
                abortWrite(l, inprogressZnodePath);
                // abort setting max sequence number
                abortStore(maxLogSegmentSequenceNo, logSegmentSeqNo);
                // abort setting max tx id
                abortStore(maxTxId, txId);
                throw new DLInterruptedException("Interrupted zookeeper transaction on starting log segment for " + getFullyQualifiedName(), ie);
            } catch (KeeperException ke) {
                // abort ledger allocate
                ledgerAllocator.abortObtain(lh);
                // abort writing inprogress znode
                abortWrite(l, inprogressZnodePath);
                // abort setting max sequence number
                abortStore(maxLogSegmentSequenceNo, logSegmentSeqNo);
                // abort setting max tx id
                abortStore(maxTxId, txId);
                throw new ZKException("Encountered zookeeper exception on starting log segment for " + getFullyQualifiedName(), ke);
            }
            LOG.info("Created inprogress log segment {} for {} : {}",
                    new Object[] { inprogressZnodeName, getFullyQualifiedName(), l });
            return new BKLogSegmentWriter(
                    getFullyQualifiedName(),
                    inprogressZnodeName,
                    conf,
                    conf.getDLLedgerMetadataLayoutVersion(),
                    lh,
                    lock,
                    txId,
                    logSegmentSeqNo,
                    scheduler,
                    orderedFuturePool,
                    statsLogger,
                    perLogStatsLogger,
                    alertStatsLogger,
                    writeLimiter,
                    featureProvider,
                    dynConf);
        } catch (IOException exc) {
            // If we haven't written an in progress node as yet, lets not fail if this was supposed
            // to be best effort, we can retry this later
            if (bestEffort && !wroteInprogressZnode) {
                return null;
            } else {
                throw exc;
            }
        }
    }

    boolean shouldStartNewSegment(BKLogSegmentWriter writer) {
        return rollingPolicy.shouldRollover(writer, lastLedgerRollingTimeMillis);
    }

    class CompleteOp extends MetadataOp<Long> {

        final String inprogressZnodeName;
        final long logSegmentSeqNo;
        final long ledgerId;
        final long firstTxId;
        final long lastTxId;
        final int recordCount;
        final long lastEntryId;
        final long lastSlotId;

        CompleteOp(String inprogressZnodeName, long logSegmentSeqNo,
                   long ledgerId, long firstTxId, long lastTxId,
                   int recordCount, long lastEntryId, long lastSlotId) {
            this.inprogressZnodeName = inprogressZnodeName;
            this.logSegmentSeqNo = logSegmentSeqNo;
            this.ledgerId = ledgerId;
            this.firstTxId = firstTxId;
            this.lastTxId = lastTxId;
            this.recordCount = recordCount;
            this.lastEntryId = lastEntryId;
            this.lastSlotId = lastSlotId;
        }

        @Override
        Long processOp() throws IOException {
            lock.acquire(DistributedReentrantLock.LockReason.COMPLETEANDCLOSE);
            try {
                completeAndCloseLogSegment(inprogressZnodeName, logSegmentSeqNo, ledgerId, firstTxId, lastTxId,
                        recordCount, lastEntryId, lastSlotId, false);
                LOG.info("Recovered {} LastTxId:{}", getFullyQualifiedName(), lastTxId);
                return lastTxId;
            } finally {
                lock.release(DistributedReentrantLock.LockReason.COMPLETEANDCLOSE);
            }
        }

        @Override
        public String toString() {
            return String.format("CompleteOp(lid=%d, (%d-%d), count=%d, lastEntry=(%d, %d))",
                    ledgerId, firstTxId, lastTxId, recordCount, lastEntryId, lastSlotId);
        }
    }

    class CompleteWriterOp extends MetadataOp<Long> {

        final BKLogSegmentWriter writer;

        CompleteWriterOp(BKLogSegmentWriter writer) {
            this.writer = writer;
        }

        @Override
        Long processOp() throws IOException {
            writer.close();
            // in theory closeToFinalize should throw exception if a stream is in error.
            // just in case, add another checking here to make sure we don't close log segment is a stream is in error.
            if (writer.shouldFailCompleteLogSegment()) {
                throw new IOException("PerStreamLogWriter for " + writer.getFullyQualifiedLogSegment() + " is already in error.");
            }
            completeAndCloseLogSegment(
                    inprogressZNodeName(writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLogSegmentSequenceNumber()),
                    writer.getLogSegmentSequenceNumber(), writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLastTxId(),
                    writer.getPositionWithinLogSegment(), writer.getLastDLSN().getEntryId(), writer.getLastDLSN().getSlotId(), true);
            return writer.getLastTxId();
        }

        @Override
        public String toString() {
            return String.format("CompleteWriterOp(lid=%d, (%d-%d), count=%d, lastEntry=(%d, %d))",
                    writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLastTxId(),
                    writer.getPositionWithinLogSegment(), writer.getLastDLSN().getEntryId(), writer.getLastDLSN().getSlotId());
        }
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
    public void completeAndCloseLogSegment(BKLogSegmentWriter writer)
        throws IOException {
        new CompleteWriterOp(writer).process();
    }

    @VisibleForTesting
    void completeAndCloseLogSegment(long logSegmentSeqNo, long ledgerId, long firstTxId, long lastTxId, int recordCount)
        throws IOException {
        completeAndCloseLogSegment(inprogressZNodeName(ledgerId, firstTxId, logSegmentSeqNo), logSegmentSeqNo,
            ledgerId, firstTxId, lastTxId, recordCount, -1, -1, true);
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
    void completeAndCloseLogSegment(String inprogressZnodeName, long logSegmentSeqNo,
                                    long ledgerId, long firstTxId, long lastTxId,
                                    int recordCount, long lastEntryId, long lastSlotId, boolean shouldReleaseLock)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            doCompleteAndCloseLogSegment(inprogressZnodeName, logSegmentSeqNo,
                                         ledgerId, firstTxId, lastTxId, recordCount,
                                         lastEntryId, lastSlotId, shouldReleaseLock);
            success = true;
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
            startSequenceId = 0L;
            for (LogSegmentMetadata metadata : logSegmentDescList) {
                if (metadata.getLogSegmentSequenceNumber() >= segment.getLogSegmentSequenceNumber()) {
                    continue;
                } else if (metadata.getLogSegmentSequenceNumber() < (segment.getLogSegmentSequenceNumber() - 1)) {
                    break;
                }
                if (metadata.isInProgress()) {

                    throw new UnexpectedException("Should not complete log segment " + segment.getLogSegmentSequenceNumber()
                            + " since it's previous log segment is still inprogress : " + logSegmentDescList);
                }
                if (metadata.supportsSequenceId()) {
                    startSequenceId = metadata.getStartSequenceId() + metadata.getRecordCount();
                }
            }
        }

        return startSequenceId;
    }

    /**
     * Close the log segment.
     *
     * @param firstTxId
     * @param lastTxId
     * @param recordCount
     * @param lastEntryId
     * @param lastSlotId
     * @param shouldReleaseLock
     * @throws IOException
     */
    protected void doCompleteAndCloseLogSegment(String inprogressZnodeName, long logSegmentSeqNo,
                                                long ledgerId, long firstTxId, long lastTxId, int recordCount,
                                                long lastEntryId, long lastSlotId, boolean shouldReleaseLock)
            throws IOException {
        checkLogExists();
        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        String inprogressZnodePath = inprogressZNode(inprogressZnodeName);
        try {
            lock.checkOwnershipAndReacquire(true);
            // for normal case, it just fetches the metadata from caches, for recovery case, it reads
            // from zookeeper.
            LogSegmentMetadata inprogressLogSegment = readLogSegmentFromCache(inprogressZnodeName);

            if (inprogressLogSegment.getLedgerId() != ledgerId) {
                throw new IOException(
                    "Active ledger has different ID to inprogress. "
                        + inprogressLogSegment.getLedgerId() + " found, "
                        + ledgerId + " expected");
            }

            if (inprogressLogSegment.getFirstTxId() != firstTxId) {
                throw new IOException("Transaction id not as expected, "
                    + inprogressLogSegment.getFirstTxId() + " found, " + firstTxId + " expected");
            }

            org.apache.zookeeper.Transaction txn = zooKeeperClient.get().transaction();

            String nameForCompletedLedger = completedLedgerZNodeName(firstTxId, lastTxId, logSegmentSeqNo);
            String pathForCompletedLedger = completedLedgerZNode(firstTxId, lastTxId, logSegmentSeqNo);

            long startSequenceId = computeStartSequenceId(inprogressLogSegment);

            // write completed ledger znode
            LogSegmentMetadata completedLogSegment =
                    inprogressLogSegment.completeLogSegment(
                            pathForCompletedLedger,
                            lastTxId,
                            recordCount,
                            lastEntryId,
                            lastSlotId,
                            startSequenceId);
            setLastLedgerRollingTimeMillis(completedLogSegment.getCompletionTime());

            // create completed log segments
            tryWrite(txn, zooKeeperClient.getDefaultACL(), completedLogSegment, pathForCompletedLedger);
            // delete inprogress log segment
            txn.delete(inprogressZnodePath, -1);
            // store max sequence number.
            long maxSeqNo= Math.max(logSegmentSeqNo, maxLogSegmentSequenceNo.getSequenceNumber());
            if (DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO == maxLogSegmentSequenceNo.getSequenceNumber()) {
                // no ledger seqno stored in /ledgers before
                LOG.warn("No max ledger sequence number found while completing log segment {} for {}.",
                         logSegmentSeqNo, inprogressZnodePath);
            } else if (maxLogSegmentSequenceNo.getSequenceNumber() != logSegmentSeqNo) {
                // ignore the case that a new inprogress log segment is pre-allocated
                // before completing current inprogress one
                if (maxLogSegmentSequenceNo.getSequenceNumber() != logSegmentSeqNo + 1) {
                    LOG.warn("Unexpected max ledger sequence number {} found while completing log segment {} for {}",
                            new Object[] { maxLogSegmentSequenceNo.getSequenceNumber(), logSegmentSeqNo, getFullyQualifiedName() });
                }
            } else {
                LOG.info("Try storing max sequence number {} in completing {}.",
                         new Object[] { logSegmentSeqNo, inprogressZnodePath });
            }
            tryStore(txn, maxLogSegmentSequenceNo, maxSeqNo);
            // update max txn id.
            LOG.debug("Trying storing LastTxId in Finalize Path {} LastTxId {}", pathForCompletedLedger, lastTxId);
            tryStore(txn, maxTxId, lastTxId);
            try {
                List<OpResult> opResults = txn.commit();
                // Confirm write completed segment
                {
                    OpResult result = opResults.get(0);
                    confirmWrite(result, completedLogSegment, pathForCompletedLedger);
                }
                // Confirm deleted inprogress segment
                {
                    // opResults.get(1);
                }
                // Updated max sequence number completed
                {
                    OpResult result = opResults.get(2);
                    confirmStore(result, maxLogSegmentSequenceNo, maxSeqNo);
                }
                // Confirm storing max tx id
                if (opResults.size() == 4) {
                    OpResult result = opResults.get(3);
                    confirmStore(result, maxTxId, lastTxId);
                }
            } catch (KeeperException ke) {
                // abort writing completed znode
                abortWrite(completedLogSegment, pathForCompletedLedger);
                // abort setting max sequence number
                abortStore(maxLogSegmentSequenceNo, maxSeqNo);
                // abort setting max tx id
                abortStore(maxTxId, lastTxId);

                List<OpResult> errorResults = ke.getResults();
                if (null != errorResults) {
                    OpResult completedLedgerResult = errorResults.get(0);
                    if (null != completedLedgerResult && ZooDefs.OpCode.error == completedLedgerResult.getType()) {
                        OpResult.ErrorResult errorResult = (OpResult.ErrorResult) completedLedgerResult;
                        if (KeeperException.Code.NODEEXISTS.intValue() == errorResult.getErr()) {
                            if (!completedLogSegment.checkEquivalence(zooKeeperClient, pathForCompletedLedger)) {
                                throw new IOException("Node " + pathForCompletedLedger + " already exists"
                                        + " but data doesn't match");
                            }
                        } else {
                            // fail on completing an inprogress log segment
                            throw ke;
                        }
                        // fall back to use synchronous calls
                        maxLogSegmentSequenceNo.store(zooKeeperClient, logMetadata.getLogSegmentsPath(), maxSeqNo);
                        maxTxId.store(lastTxId);
                        LOG.info("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressZnodePath, lastTxId);
                        try {
                            zooKeeperClient.get().delete(inprogressZnodePath, -1);
                        } catch (KeeperException.NoNodeException nne) {
                            LOG.warn("No inprogress log segment {} to delete while completing log segment {} for {} : ",
                                     new Object[] { inprogressZnodeName, logSegmentSeqNo, getFullyQualifiedName(), nne });
                        }
                    } else {
                        throw ke;
                    }
                } else {
                    LOG.warn("No OpResults for a multi operation when finalising stream " + logMetadata.getLogRootPath(), ke);
                    throw ke;
                }
            }
            removeLogSegmentFromCache(inprogressZnodeName);
            addLogSegmentToCache(nameForCompletedLedger, completedLogSegment);
            LOG.info("Completed {} to {} for {} : {}",
                     new Object[] { inprogressZnodeName, nameForCompletedLedger, getFullyQualifiedName(), completedLogSegment });
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted when finalising stream " + logMetadata.getLogRootPath(), e);
        } catch (KeeperException e) {
            throw new ZKException("Error when finalising stream " + logMetadata.getLogRootPath(), e);
        } finally {
            if (shouldReleaseLock && startLogSegmentCount.get() > 0) {
                lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
                startLogSegmentCount.decrementAndGet();
            }
        }
    }

    public synchronized long recoverIncompleteLogSegments() throws IOException {
        FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_RecoverIncompleteLogSegments);
        return new RecoverOp().process();
    }

    class RecoverOp extends MetadataOp<Long> {

        @Override
        Long processOp() throws IOException {
            Stopwatch stopwatch = Stopwatch.createStarted();
            boolean success = false;

            try {
                long lastTxId;
                synchronized (BKLogWriteHandler.this) {
                    lastTxId = doProcessOp();
                }
                success = true;
                return lastTxId;
            } finally {
                if (success) {
                    recoverOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                } else {
                    recoverOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                }
            }
        }

        long doProcessOp() throws IOException {
            List<LogSegmentMetadata> segmentList = getFilteredLedgerList(false, false);
            LOG.info("Initiating Recovery For {} : {}", getFullyQualifiedName(), segmentList);
            // if lastLedgerRollingTimeMillis is not updated, we set it to now.
            synchronized (BKLogWriteHandler.this) {
                if (lastLedgerRollingTimeMillis < 0) {
                    lastLedgerRollingTimeMillis = Utils.nowInMillis();
                }
            }
            long lastTxId = DistributedLogConstants.INVALID_TXID;
            for (LogSegmentMetadata l : segmentList) {
                if (!l.isInProgress()) {
                    lastTxId = Math.max(lastTxId, l.getLastTxId());
                    continue;
                }
                long recoveredTxId = new RecoverLogSegmentOp(l).process();
                lastTxId = Math.max(lastTxId, recoveredTxId);
            }
            return lastTxId;
        }

        @Override
        public String toString() {
            return String.format("RecoverOp(%s)", getFullyQualifiedName());
        }
    }

    class RecoverLogSegmentOp extends MetadataOp<Long> {

        final LogSegmentMetadata l;

        RecoverLogSegmentOp(LogSegmentMetadata l) {
            this.l = l;
        }

        @Override
        Long processOp() throws IOException {
            long endTxId = DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID;
            int recordCount = 0;
            long lastEntryId = -1;
            long lastSlotId = -1;

            LogRecordWithDLSN record;
            lock.acquire(DistributedReentrantLock.LockReason.RECOVER);
            try {
                LOG.info("Recovering last record in log segment {} for {}.", l, getFullyQualifiedName());
                record = recoverLastRecordInLedger(l, true, true, true);
                LOG.info("Recovered last record in log segment {} for {}.", l, getFullyQualifiedName());
            } finally {
                lock.release(DistributedReentrantLock.LockReason.RECOVER);
            }

            if (null != record) {
                endTxId = record.getTransactionId();
                recordCount = record.getPositionWithinLogSegment();
                lastEntryId = record.getDlsn().getEntryId();
                lastSlotId = record.getDlsn().getSlotId();
            }

            if (endTxId == DistributedLogConstants.INVALID_TXID) {
                LOG.error("Unrecoverable corruption has occurred in segment "
                    + l.toString() + " at path " + l.getZkPath()
                    + ". Unable to continue recovery.");
                throw new IOException("Unrecoverable corruption,"
                    + " please check logs.");
            } else if (endTxId == DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID) {
                // TODO: Empty ledger - Ideally we should just remove it?
                endTxId = l.getFirstTxId();
            }

            CompleteOp completeOp = new CompleteOp(
                    l.getZNodeName(), l.getLogSegmentSequenceNumber(),
                    l.getLedgerId(), l.getFirstTxId(), endTxId,
                    recordCount, lastEntryId, lastSlotId);
            return completeOp.process();
        }

        @Override
        public String toString() {
            return String.format("RecoverLogSegmentOp(%s)", l);
        }
    }

    public void deleteLog() throws IOException {
        try {
            checkLogExists();
        } catch (DLInterruptedException die) {
            throw die;
        } catch (IOException exc) {
            return;
        }

        try {
            deleteLock.acquire(DistributedReentrantLock.LockReason.DELETELOG);
        } catch (LockingException lockExc) {
            throw new IOException("deleteLog could not acquire exclusive lock on the log " + getFullyQualifiedName());
        }

        try {
            FutureUtils.result(purgeLogSegmentsOlderThanTxnId(-1));
        } finally {
            deleteLock.release(DistributedReentrantLock.LockReason.DELETELOG);
        }

        try {
            lock.close();
            deleteLock.close();
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

    public void close() {
        closed = true;
        if (startLogSegmentCount.get() > 0) {
            while (startLogSegmentCount.decrementAndGet() >= 0) {
                lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
            }
        }
        if (lockHandler) {
            lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
        }
        lock.close();
        deleteLock.close();
        ledgerAllocator.close(false);
        // close the zookeeper client & bookkeeper client after closing the lock
        super.close();
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
