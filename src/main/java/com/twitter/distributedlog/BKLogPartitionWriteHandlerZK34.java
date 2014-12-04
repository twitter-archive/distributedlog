package com.twitter.distributedlog;

import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.ZKException;

import com.twitter.distributedlog.util.PermitLimiter;

import com.twitter.distributedlog.zk.DataWithStat;
import com.twitter.util.FuturePool;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Charsets.UTF_8;

class BKLogPartitionWriteHandlerZK34 extends BKLogPartitionWriteHandler {

    protected final LedgerAllocator ledgerAllocator;
    protected final boolean ownAllocator;

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
                                   OrderedSafeExecutor lockStateExecutor,
                                   LedgerAllocator allocator,
                                   StatsLogger statsLogger,
                                   String clientId,
                                   int regionId,
                                   PermitLimiter writeLimiter) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder,
              executorService, orderedFuturePool, metadataExecutor, lockStateExecutor,
              allocator, true, statsLogger, clientId, regionId, writeLimiter);
        // Construct ledger allocator
        if (null == allocator) {
            ledgerAllocator = new SimpleLedgerAllocator(allocationPath, allocationData, conf, zooKeeperClient, bookKeeperClient);
            ledgerAllocator.start();
            ownAllocator = true;
        } else {
            ledgerAllocator = allocator;
            ownAllocator = false;
        }
    }

    // Transactional operations for MaxLedgerSequenceNo
    void tryStore(Transaction txn, MaxLedgerSequenceNo maxSeqNo, long seqNo) {
        byte[] data = MaxLedgerSequenceNo.toBytes(seqNo);
        txn.setData(ledgerPath, data, maxSeqNo.getZkVersion());
    }

    void confirmStore(OpResult result, MaxLedgerSequenceNo maxSeqNo, long seqNo) {
        assert (result instanceof OpResult.SetDataResult);
        OpResult.SetDataResult setDataResult = (OpResult.SetDataResult) result;
        maxSeqNo.update(setDataResult.getStat().getVersion(), seqNo);
    }

    void abortStore(MaxLedgerSequenceNo maxSeqNo, long seqNo) {
        // nop
    }

    // Transactional operations for MaxTxId
    void tryStore(Transaction txn, MaxTxId maxTxId, long txId) throws IOException {
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
    void tryWrite(Transaction txn, List<ACL> acl, LogSegmentLedgerMetadata metadata, String path) {
        byte[] finalisedData = metadata.getFinalisedData().getBytes(UTF_8);
        txn.create(path, finalisedData, acl, CreateMode.PERSISTENT);
    }

    void confirmWrite(OpResult result, LogSegmentLedgerMetadata metadata, String path) {
        metadata.setZkPath(path);
    }

    void abortWrite(LogSegmentLedgerMetadata metadata, String path) {
        // nop
    }

    @Override
    protected void getLedgersData(DataWithStat ledgersData) throws IOException {
        final String ledgerPath = partitionRootPath + "/ledgers";
        Stat stat = new Stat();
        try {
            byte[] data = zooKeeperClient.get().getData(ledgerPath, false, stat);
            ledgersData.setDataWithStat(data, stat);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on getting ledgers data : " + ledgerPath, ie);
        } catch (KeeperException.NoNodeException nne) {
            throw new LogNotFoundException("No /ledgers found for stream " + getFullyQualifiedName());
        } catch (KeeperException ke) {
            throw new ZKException("Failed on getting ledgers data " + ledgerPath + " : ", ke);
        }
    }

    @Override
    protected BKPerStreamLogWriter doStartLogSegment(long txId, boolean bestEffort, boolean allowMaxTxID) throws IOException {
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
            Transaction txn;
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

            // For any active stream we will always make sure that there is at least one
            // active ledger (except when the stream first starts out). Therefore when we
            // see no ledger metadata for a stream, we assume that this is the first ledger
            // in the stream
            long ledgerSeqNo = DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO;

            if (LogSegmentLedgerMetadata.supportsLedgerSequenceNo(conf.getDLLedgerMetadataLayoutVersion())) {
                List<LogSegmentLedgerMetadata> ledgerListDesc = getFilteredLedgerListDesc(false, false);
                ledgerSeqNo = DistributedLogConstants.FIRST_LEDGER_SEQNO;
                if (!ledgerListDesc.isEmpty()) {
                    ledgerSeqNo = ledgerListDesc.get(0).getLedgerSequenceNumber() + 1;
                }
            }

            if (DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO == maxLedgerSequenceNo.getSequenceNumber()) {
                // no ledger seqno stored in /ledgers before
                LOG.warn("No max ledger sequence number found while creating log segment {} for {}.",
                        ledgerSeqNo, getFullyQualifiedName());
            } else if (maxLedgerSequenceNo.getSequenceNumber() + 1 != ledgerSeqNo) {
                LOG.warn("Unexpected max log segment sequence number {} for {} : list of cached segments = {}",
                         new Object[] { maxLedgerSequenceNo.getSequenceNumber(), getFullyQualifiedName(),
                                 getCachedFullLedgerList(LogSegmentLedgerMetadata.DESC_COMPARATOR) });
                // there is max log segment number recorded there and it isn't match. throw exception.
                throw new DLIllegalStateException("Unexpected max log segment sequence number "
                        + maxLedgerSequenceNo.getSequenceNumber() + " for " + getFullyQualifiedName()
                        + ", expected " + (ledgerSeqNo - 1));
            }

            String inprogressZnodeName = inprogressZNodeName(lh.getId(), txId, ledgerSeqNo);
            String inprogressZnodePath = inprogressZNode(lh.getId(), txId, ledgerSeqNo);
            LogSegmentLedgerMetadata l =
                new LogSegmentLedgerMetadata.LogSegmentLedgerMetadataBuilder(inprogressZnodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), txId)
                    .setLedgerSequenceNo(ledgerSeqNo)
                    .setRegionId(regionId).build();
            tryWrite(txn, zooKeeperClient.getDefaultACL(), l, inprogressZnodePath);

            // Try storing max sequence number.
            LOG.debug("Try storing max sequence number in startLogSegment {} : {}", inprogressZnodePath, ledgerSeqNo);
            tryStore(txn, maxLedgerSequenceNo, ledgerSeqNo);

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
                    confirmStore(result, maxLedgerSequenceNo, ledgerSeqNo);
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
                abortStore(maxLedgerSequenceNo, ledgerSeqNo);
                // abort setting max tx id
                abortStore(maxTxId, txId);
                throw new DLInterruptedException("Interrupted zookeeper transaction on starting log segment for " + getFullyQualifiedName(), ie);
            } catch (KeeperException ke) {
                // abort ledger allocate
                ledgerAllocator.abortObtain(lh);
                // abort writing inprogress znode
                abortWrite(l, inprogressZnodePath);
                // abort setting max sequence number
                abortStore(maxLedgerSequenceNo, ledgerSeqNo);
                // abort setting max tx id
                abortStore(maxTxId, txId);
                throw new ZKException("Encountered zookeeper exception on starting log segment for " + getFullyQualifiedName(), ke);
            }
            LOG.info("Created inprogress log segment {} for {} : {}",
                    new Object[] { inprogressZnodeName, getFullyQualifiedName(), l });
            return new BKPerStreamLogWriter(getFullyQualifiedName(), inprogressZnodeName, conf,
                lh, lock, txId, ledgerSeqNo, executorService, orderedFuturePool, statsLogger, writeLimiter);
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

    @Override
    protected void doCompleteAndCloseLogSegment(String inprogressZnodeName, long ledgerSeqNo,
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
            // create completed log segments
            tryWrite(txn, zooKeeperClient.getDefaultACL(), logSegment, pathForCompletedLedger);
            // delete inprogress log segment
            txn.delete(inprogressZnodePath, -1);
            // store max sequence number.
            long maxSeqNo= Math.max(ledgerSeqNo, maxLedgerSequenceNo.getSequenceNumber());
            if (DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO == maxLedgerSequenceNo.getSequenceNumber()) {
                // no ledger seqno stored in /ledgers before
                LOG.warn("No max ledger sequence number found while completing log segment {} for {}.",
                         ledgerSeqNo, inprogressZnodePath);
            } else if (maxLedgerSequenceNo.getSequenceNumber() != ledgerSeqNo) {
                LOG.warn("Unexpected max ledger sequence number {} found while completing log segment {} for {}",
                        new Object[] { maxLedgerSequenceNo.getSequenceNumber(), ledgerSeqNo, getFullyQualifiedName()  });
            } else {
                LOG.info("Try storing max sequence number {} in completing {}.",
                         new Object[] { ledgerSeqNo, inprogressZnodePath });
            }
            tryStore(txn, maxLedgerSequenceNo, maxSeqNo);
            // update max txn id.
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
                // Updated max sequence number completed
                {
                    OpResult result = opResults.get(2);
                    confirmStore(result, maxLedgerSequenceNo, maxSeqNo);
                }
                // Confirm storing max tx id
                if (opResults.size() == 4) {
                    OpResult result = opResults.get(3);
                    confirmStore(result, maxTxId, lastTxId);
                }
            } catch (KeeperException ke) {
                // abort writing completed znode
                abortWrite(logSegment, pathForCompletedLedger);
                // abort setting max sequence number
                abortStore(maxLedgerSequenceNo, maxSeqNo);
                // abort setting max tx id
                abortStore(maxTxId, lastTxId);

                List<OpResult> errorResults = ke.getResults();
                if (null != errorResults) {
                    OpResult completedLedgerResult = errorResults.get(0);
                    if (null != completedLedgerResult && ZooDefs.OpCode.error == completedLedgerResult.getType()) {
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
                        maxLedgerSequenceNo.store(zooKeeperClient, ledgerPath, maxSeqNo);
                        maxTxId.store(lastTxId);
                        LOG.info("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressZnodePath, lastTxId);
                        try {
                            zooKeeperClient.get().delete(inprogressZnodePath, -1);
                        } catch (KeeperException.NoNodeException nne) {
                            LOG.warn("No inprogress log segment {} to delete while completing log segment {} for {} : ",
                                     new Object[] { inprogressZnodeName, ledgerSeqNo, getFullyQualifiedName(), nne });
                        }
                    } else {
                        throw ke;
                    }
                } else {
                    LOG.warn("No OpResults for a multi operation when finalising stream " + partitionRootPath, ke);
                    throw ke;
                }
            }
            removeLogSegmentFromCache(inprogressZnodeName);
            addLogSegmentToCache(nameForCompletedLedger, logSegment);
            LOG.info("Completed {} to {} for {} : {}",
                     new Object[] { inprogressZnodeName, nameForCompletedLedger, getFullyQualifiedName(), logSegment });
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted when finalising stream " + partitionRootPath, e);
        } catch (KeeperException e) {
            throw new ZKException("Error when finalising stream " + partitionRootPath, e);
        } finally {
            if (shouldReleaseLock && startLogSegmentCount.get() > 0) {
                lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
                startLogSegmentCount.decrementAndGet();
            }
        }
    }

    @Override
    public void close() {
        if (ownAllocator) {
            ledgerAllocator.close(false);
        }
        super.close();
    }
}
