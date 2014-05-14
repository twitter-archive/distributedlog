package com.twitter.distributedlog;

import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.ZKException;

import com.twitter.util.FuturePool;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Charsets.UTF_8;

class BKLogPartitionWriteHandlerZK34 extends BKLogPartitionWriteHandler {

    protected final LedgerAllocator ledgerAllocator;

    /**
     * Construct ZK34 based write handler.
     *
     * @param name
     * @param streamIdentifier
     * @param conf
     * @param uri
     * @param zkcBuilder
     * @param bkcBuilder
     * @param executorService
     * @param statsLogger
     * @param clientId
     */
    BKLogPartitionWriteHandlerZK34(String name,
                                   String streamIdentifier,
                                   DistributedLogConfiguration conf,
                                   URI uri,
                                   ZooKeeperClientBuilder zkcBuilder,
                                   BookKeeperClientBuilder bkcBuilder,
                                   ScheduledExecutorService executorService,
                                   FuturePool orderedFuturePool,
                                   ExecutorService metadataExecutor,
                                   LedgerAllocator allocator,
                                   StatsLogger statsLogger,
                                   String clientId,
                                   int regionId) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder,
              executorService, orderedFuturePool, metadataExecutor, allocator, true, statsLogger, clientId, regionId);
        // Construct ledger allocator
        if (null == allocator) {
            ledgerAllocator = new SimpleLedgerAllocator(allocationPath, allocationData, conf, zooKeeperClient, bookKeeperClient);
            ledgerAllocator.start();
        } else {
            ledgerAllocator = allocator;
        }
    }

    // Transactional operations for MaxTxId
    void tryStore(Transaction txn, MaxTxId maxTxId, long txId) throws IOException{
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
    void tryWrite(Transaction txn, LogSegmentLedgerMetadata metadata, String path) {
        byte[] finalisedData = metadata.getFinalisedData().getBytes(UTF_8);
        txn.create(path, finalisedData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    void confirmWrite(OpResult result, LogSegmentLedgerMetadata metadata, String path) {
        metadata.setZkPath(path);
    }

    void abortWrite(LogSegmentLedgerMetadata metadata, String path) {
        // nop
    }

    @Override
    protected BKPerStreamLogWriter doStartLogSegment(long txId, boolean bestEffort) throws IOException {
        checkLogExists();

        boolean wroteInprogressZnode = false;

        try {
            if ((txId < 0) ||
                    (txId == DistributedLogConstants.MAX_TXID)) {
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
            Transaction txn;
            try {
                txn = zooKeeperClient.get().transaction();
            } catch (InterruptedException e) {
                LOG.error("Interrupted on obtaining a zookeeper transaction for starting log segment on {} : ",
                        getFullyQualifiedName(), e);
                throw new DLInterruptedException("Interrupted on obtaining a zookeeper transaction for starting log segment on "
                        + getFullyQualifiedName(), e);
            }

            // Try obtaining an new ledger
            LedgerHandle lh = ledgerAllocator.tryObtain(txn);

            // For any active stream we will always make sure that there is at least one
            // active ledger (except when the stream first starts out). Therefore when we
            // see no ledger metadata for a stream, we assume that this is the first ledger
            // in the stream
            long ledgerSeqNo = DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO;

            if (conf.getDLLedgerMetadataLayoutVersion() >=
                    DistributedLogConstants.FIRST_LEDGER_METADATA_VERSION_FOR_LEDGER_SEQNO) {
                List<LogSegmentLedgerMetadata> ledgerListDesc = getFilteredLedgerListDesc(false, false);
                ledgerSeqNo = DistributedLogConstants.FIRST_LEDGER_SEQNO;
                if (!ledgerListDesc.isEmpty()) {
                    ledgerSeqNo = ledgerListDesc.get(0).getLedgerSequenceNumber() + 1;
                }
            }

            String inprogressZnodeName = inprogressZNodeName(lh.getId(), txId, ledgerSeqNo);
            String inprogressZnodePath = inprogressZNode(lh.getId(), txId, ledgerSeqNo);
            LogSegmentLedgerMetadata l = new LogSegmentLedgerMetadata(inprogressZnodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), txId, ledgerSeqNo, regionId);
            tryWrite(txn, l, inprogressZnodePath);

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
                // Storing max tx id completed.
                if (resultList.size() == 3) {
                    // get the result of storing max tx id.
                    OpResult result = resultList.get(2);
                    confirmStore(result, maxTxId, txId);
                }
            } catch (InterruptedException ie) {
                // abort ledger allocate
                ledgerAllocator.abortObtain(lh);
                // abort writing inprogress znode
                abortWrite(l, inprogressZnodePath);
                // abort setting max tx id
                abortStore(maxTxId, txId);
                throw new DLInterruptedException("Interrupted zookeeper transaction on starting log segment for " + getFullyQualifiedName(), ie);
            } catch (KeeperException ke) {
                // abort ledger allocate
                ledgerAllocator.abortObtain(lh);
                // abort writing inprogress znode
                abortWrite(l, inprogressZnodePath);
                // abort setting max tx id
                abortStore(maxTxId, txId);
                throw new ZKException("Encountered zookeeper exception on starting log segment for " + getFullyQualifiedName(), ke);
            }
            return new BKPerStreamLogWriter(getFullyQualifiedName(), inprogressZnodeName, conf,
                lh, lock, txId, ledgerSeqNo, executorService, orderedFuturePool, statsLogger);
        } catch (Exception exc) {
            // If we haven't written an in progress node as yet, lets not fail if this was supposed
            // to be best effort, we can retry this later
            if (bestEffort && !wroteInprogressZnode) {
                return null;
            } else if (exc instanceof IOException) {
                throw (IOException) exc;
            } else {
                throw new IOException("Error creating new log segment", exc);
            }
        }
    }

    @Override
    protected void doCompleteAndCloseLogSegment(String inprogressZnodeName, long ledgerSeqNo,
                                                long ledgerId, long firstTxId, long lastTxId, int recordCount,
                                                long lastEntryId, long lastSlotId, boolean shouldReleaseLock)
            throws IOException {
        checkLogExists();
        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        String inprogressZnodePath = inprogressZNode(inprogressZnodeName);
        boolean acquiredLocally = false;
        try {
            acquiredLocally = lock.checkWriteLock(true, DistributedReentrantLock.LockReason.COMPLETEANDCLOSE);
            // for normal case, it just fetches the metadata from caches, for recovery case, it reads
            // from zookeeper.
            LogSegmentLedgerMetadata logSegment = readLogSegmentFromCache(inprogressZnodeName);

            if (logSegment.getLedgerId() != ledgerId) {
                throw new IOException(
                    "Active ledger has different ID to inprogress. "
                        + logSegment.getLedgerId() + " found, "
                        + ledgerId + " expected");
            }

            if (logSegment.getFirstTxId() != firstTxId) {
                throw new IOException("Transaction id not as expected, "
                    + logSegment.getFirstTxId() + " found, " + firstTxId + " expected");
            }

            Transaction txn = zooKeeperClient.get().transaction();

            // write completed ledger znode
            setLastLedgerRollingTimeMillis(logSegment.finalizeLedger(lastTxId, recordCount, lastEntryId, lastSlotId));
            String nameForCompletedLedger = completedLedgerZNodeName(ledgerId, firstTxId, lastTxId, ledgerSeqNo);
            String pathForCompletedLedger = completedLedgerZNode(ledgerId, firstTxId, lastTxId, ledgerSeqNo);
            tryWrite(txn, logSegment, pathForCompletedLedger);
            txn.delete(inprogressZnodePath, -1);
            LOG.debug("Trying storing LastTxId in Finalize Path {} LastTxId {}", pathForCompletedLedger, lastTxId);
            tryStore(txn, maxTxId, lastTxId);
            try {
                List<OpResult> opResults = txn.commit();
                // Confirm write completed segment
                {
                    OpResult result = opResults.get(0);
                    confirmWrite(result, logSegment, pathForCompletedLedger);
                }
                // Confirm deleted inprogress segment
                {
                    // opResults.get(1);
                }
                // Confirm storing max tx id
                if (opResults.size() == 3) {
                    OpResult result = opResults.get(2);
                    confirmStore(result, maxTxId, lastTxId);
                }
            } catch (KeeperException ke) {
                List<OpResult> errorResults = ke.getResults();
                OpResult completedLedgerResult = errorResults.get(0);
                if (ZooDefs.OpCode.error == completedLedgerResult.getType()) {
                    OpResult.ErrorResult errorResult = (OpResult.ErrorResult) completedLedgerResult;
                    if (KeeperException.Code.NODEEXISTS.intValue() == errorResult.getErr()) {
                        if (!logSegment.checkEquivalence(zooKeeperClient, pathForCompletedLedger)) {
                            throw new IOException("Node " + pathForCompletedLedger + " already exists"
                                    + " but data doesn't match");
                        }
                    } else {
                        // fail on completing an inprogress log segment
                        throw ke;
                    }
                    // fall back to use synchronous calls
                    maxTxId.store(lastTxId);
                    LOG.debug("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressZnodePath, lastTxId);
                    zooKeeperClient.get().delete(inprogressZnodePath, -1);
                } else {
                    throw ke;
                }
            }
            removeLogSegmentToCache(inprogressZnodeName);
            addLogSegmentToCache(nameForCompletedLedger, logSegment);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when finalising stream " + partitionRootPath, e);
        } catch (KeeperException e) {
            throw new IOException("Error when finalising stream " + partitionRootPath, e);
        } finally {
            if (acquiredLocally || (shouldReleaseLock && startLogSegmentCount.get() > 0)) {
                DistributedReentrantLock.LockReason reason = acquiredLocally ?
                    DistributedReentrantLock.LockReason.COMPLETEANDCLOSE:
                    DistributedReentrantLock.LockReason.WRITEHANDLER;
                lock.release(reason);
                startLogSegmentCount.decrementAndGet();
            }
        }
    }
}
