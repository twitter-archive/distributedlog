package com.twitter.distributedlog.bk;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.distributedlog.util.Transaction.OpListener;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import com.twitter.distributedlog.zk.ZKTransaction;
import com.twitter.distributedlog.zk.ZKVersionedSetOp;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Allocator to allocate ledgers.
 */
public class SimpleLedgerAllocator implements LedgerAllocator, FutureEventListener<LedgerHandle>, OpListener<Version> {

    static final Logger LOG = LoggerFactory.getLogger(SimpleLedgerAllocator.class);

    static enum Phase {
        ALLOCATING, ALLOCATED, HANDING_OVER, HANDED_OVER, ERROR
    }

    static class AllocationException extends IOException {

        private static final long serialVersionUID = -1111397872059426882L;

        private final Phase phase;

        public AllocationException(Phase phase, String msg) {
            super(msg);
            this.phase = phase;
        }

        public Phase getPhase() {
            return this.phase;
        }

    }

    static class ConcurrentObtainException extends AllocationException {

        private static final long serialVersionUID = -8532471098537176913L;

        public ConcurrentObtainException(Phase phase, String msg) {
            super(phase, msg);
        }
    }

    // zookeeper client
    final ZooKeeperClient zkc;
    // bookkeeper client
    final BookKeeperClient bkc;
    // znode path
    final String allocatePath;
    // allocation phase
    Phase phase = Phase.HANDED_OVER;
    // version
    ZkVersion version = new ZkVersion(-1);
    // outstanding allocation
    Promise<LedgerHandle> allocatePromise;
    // outstanding tryObtain transaction
    Transaction<Object> tryObtainTxn = null;
    OpListener<LedgerHandle> tryObtainListener = null;
    // ledger id left from previous allocation
    Long ledgerIdLeftFromPrevAllocation = null;
    // Allocated Ledger
    LedgerHandle allocatedLh = null;

    Future<Void> closeFuture = null;
    final LinkedList<Future<Void>> ledgerDeletions =
            new LinkedList<Future<Void>>();

    // Ledger configuration
    private final QuorumConfigProvider quorumConfigProvider;

    static Future<Versioned<byte[]>> getAndCreateAllocationData(final String allocatePath,
                                                                final ZooKeeperClient zkc) {
        return Utils.zkGetData(zkc, allocatePath, false)
                .flatMap(new AbstractFunction1<Versioned<byte[]>, Future<Versioned<byte[]>>>() {
            @Override
            public Future<Versioned<byte[]>> apply(Versioned<byte[]> result) {
                if (null != result && null != result.getVersion() && null != result.getValue()) {
                    return Future.value(result);
                }
                return createAllocationData(allocatePath, zkc);
            }
        });
    }

    private static Future<Versioned<byte[]>> createAllocationData(final String allocatePath,
                                                                  final ZooKeeperClient zkc) {
        try {
            final Promise<Versioned<byte[]>> promise = new Promise<Versioned<byte[]>>();
            zkc.get().create(allocatePath, DistributedLogConstants.EMPTY_BYTES,
                    zkc.getDefaultACL(), CreateMode.PERSISTENT,
                    new org.apache.zookeeper.AsyncCallback.Create2Callback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
                            if (KeeperException.Code.OK.intValue() == rc) {
                                promise.setValue(new Versioned<byte[]>(DistributedLogConstants.EMPTY_BYTES,
                                        new ZkVersion(stat.getVersion())));
                            } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                                Utils.zkGetData(zkc, allocatePath, false).proxyTo(promise);
                            } else {
                                promise.setException(FutureUtils.zkException(
                                        KeeperException.create(KeeperException.Code.get(rc)), allocatePath));
                            }
                        }
                    }, null);
            return promise;
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return Future.exception(FutureUtils.zkException(e, allocatePath));
        } catch (InterruptedException e) {
            return Future.exception(FutureUtils.zkException(e, allocatePath));
        }
    }

    public static Future<SimpleLedgerAllocator> of(final String allocatePath,
                                                   final Versioned<byte[]> allocationData,
                                                   final QuorumConfigProvider quorumConfigProvider,
                                                   final ZooKeeperClient zkc,
                                                   final BookKeeperClient bkc) {
        if (null != allocationData && null != allocationData.getValue()
                && null != allocationData.getVersion()) {
            return Future.value(new SimpleLedgerAllocator(allocatePath, allocationData,
                    quorumConfigProvider, zkc, bkc));
        }
        return getAndCreateAllocationData(allocatePath, zkc)
                .map(new AbstractFunction1<Versioned<byte[]>, SimpleLedgerAllocator>() {
            @Override
            public SimpleLedgerAllocator apply(Versioned<byte[]> allocationData) {
                return new SimpleLedgerAllocator(allocatePath, allocationData,
                        quorumConfigProvider, zkc, bkc);
            }
        });
    }

    /**
     * Construct a ledger allocator.
     *
     * @param allocatePath
     *          znode path to store the allocated ledger.
     * @param allocationData
     *          allocation data.
     * @param quorumConfigProvider
     *          Quorum configuration provider.
     * @param zkc
     *          zookeeper client.
     * @param bkc
     *          bookkeeper client.
     */
    public SimpleLedgerAllocator(String allocatePath,
                                 Versioned<byte[]> allocationData,
                                 QuorumConfigProvider quorumConfigProvider,
                                 ZooKeeperClient zkc,
                                 BookKeeperClient bkc) {
        this.zkc = zkc;
        this.bkc = bkc;
        this.allocatePath = allocatePath;
        this.quorumConfigProvider = quorumConfigProvider;
        initialize(allocationData);
    }

    /**
     * Initialize the allocator.
     *
     * @param allocationData
     *          Allocation Data.
     */
    private void initialize(Versioned<byte[]> allocationData) {
        setVersion((ZkVersion) allocationData.getVersion());
        byte[] data = allocationData.getValue();
        if (null != data && data.length > 0) {
            // delete the allocated ledger since this is left by last allocation.
            try {
                ledgerIdLeftFromPrevAllocation = DLUtils.bytes2LedgerId(data);
            } catch (NumberFormatException nfe) {
                LOG.warn("Invalid data found in allocator path {} : ", allocatePath, nfe);
            }
        }

    }

    private synchronized void deleteLedgerLeftFromPreviousAllocationIfNecessary() {
        if (null != ledgerIdLeftFromPrevAllocation) {
            LOG.info("Deleting allocated-but-unused ledger left from previous allocation {}.", ledgerIdLeftFromPrevAllocation);
            deleteLedger(ledgerIdLeftFromPrevAllocation);
            ledgerIdLeftFromPrevAllocation = null;
        }
    }

    @Override
    public synchronized void allocate() throws IOException {
        if (Phase.ERROR == phase) {
            throw new AllocationException(Phase.ERROR, "Error on ledger allocator for " + allocatePath);
        }
        if (Phase.HANDED_OVER == phase) {
            // issue an allocate request when ledger is already handed over.
            allocateLedger();
        }
    }

    @Override
    public synchronized Future<LedgerHandle> tryObtain(final Transaction<Object> txn,
                                                       final OpListener<LedgerHandle> listener) {
        if (Phase.ERROR == phase) {
            return Future.exception(new AllocationException(Phase.ERROR,
                    "Error on allocating ledger under " + allocatePath));
        }
        if (Phase.HANDING_OVER == phase || Phase.HANDED_OVER == phase || null != tryObtainTxn) {
            return Future.exception(new ConcurrentObtainException(phase,
                    "Ledger handle is handling over to another thread : " + phase));
        }
        tryObtainTxn = txn;
        tryObtainListener = listener;
        if (null != allocatedLh) {
            completeAllocation(allocatedLh);
        }
        return allocatePromise;
    }

    @Override
    public void onCommit(Version r) {
        confirmObtain((ZkVersion) r);
    }

    private void confirmObtain(ZkVersion zkVersion) {
        boolean shouldAllocate = false;
        OpListener<LedgerHandle> listenerToNotify = null;
        LedgerHandle lhToNotify = null;
        synchronized (this) {
            if (Phase.HANDING_OVER == phase) {
                setPhase(Phase.HANDED_OVER);
                setVersion(zkVersion);
                listenerToNotify = tryObtainListener;
                lhToNotify = allocatedLh;
                // reset the state
                allocatedLh = null;
                allocatePromise = null;
                tryObtainTxn = null;
                tryObtainListener = null;
                // mark flag to issue an allocation request
                shouldAllocate = true;
            }
        }
        if (null != listenerToNotify && null != lhToNotify) {
            // notify the listener
            listenerToNotify.onCommit(lhToNotify);
        }
        if (shouldAllocate) {
            // issue an allocation request
            allocateLedger();
        }
    }

    @Override
    public void onAbort(Throwable t) {
        OpListener<LedgerHandle> listenerToNotify;
        synchronized (this) {
            listenerToNotify = tryObtainListener;
            if (t instanceof KeeperException &&
                    ((KeeperException) t).code() == KeeperException.Code.BADVERSION) {
                LOG.info("Set ledger allocator {} to ERROR state after hit bad version : version = {}",
                        allocatePath, getVersion());
                setPhase(Phase.ERROR);
            } else {
                if (Phase.HANDING_OVER == phase) {
                    setPhase(Phase.ALLOCATED);
                    tryObtainTxn = null;
                    tryObtainListener = null;
                }
            }
        }
        if (null != listenerToNotify) {
            listenerToNotify.onAbort(t);
        }
    }

    private synchronized void setPhase(Phase phase) {
        this.phase = phase;
        LOG.info("Ledger allocator {} moved to phase {} : version = {}.",
                new Object[] { allocatePath, phase, version });
    }

    private synchronized void allocateLedger() {
        // make sure previous allocation is already handed over.
        if (Phase.HANDED_OVER != phase) {
            LOG.error("Trying allocate ledger for {} in phase {}, giving up.", allocatePath, phase);
            return;
        }
        setPhase(Phase.ALLOCATING);
        allocatePromise = new Promise<LedgerHandle>();
        QuorumConfig quorumConfig = quorumConfigProvider.getQuorumConfig();
        bkc.createLedger(
                quorumConfig.getEnsembleSize(),
                quorumConfig.getWriteQuorumSize(),
                quorumConfig.getAckQuorumSize()
        ).addEventListener(this);
    }

    private synchronized void completeAllocation(LedgerHandle lh) {
        allocatedLh = lh;
        if (null == tryObtainTxn) {
            return;
        }
        org.apache.zookeeper.Op zkSetDataOp = org.apache.zookeeper.Op.setData(
                allocatePath, DistributedLogConstants.EMPTY_BYTES, version.getZnodeVersion());
        ZKVersionedSetOp commitOp = new ZKVersionedSetOp(zkSetDataOp, this);
        tryObtainTxn.addOp(commitOp);
        setPhase(Phase.HANDING_OVER);
        FutureUtils.setValue(allocatePromise, lh);
    }

    private synchronized void failAllocation(Throwable cause) {
        FutureUtils.setException(allocatePromise, cause);
    }

    @Override
    public void onSuccess(LedgerHandle lh) {
        // a ledger is created, update the ledger to allocation path before handling it over for usage.
        markAsAllocated(lh);
    }

    @Override
    public void onFailure(Throwable cause) {
        LOG.error("Error creating ledger for allocating {} : ", allocatePath, cause);
        setPhase(Phase.ERROR);
        failAllocation(cause);
    }

    private synchronized ZkVersion getVersion() {
        return version;
    }

    private synchronized void setVersion(ZkVersion newVersion) {
        Version.Occurred occurred = newVersion.compare(version);
        if (occurred == Version.Occurred.AFTER) {
            LOG.info("Ledger allocator for {} moved version from {} to {}.",
                    new Object[] { allocatePath, version, newVersion });
            version = newVersion;
        } else {
            LOG.warn("Ledger allocator for {} received an old version {}, current version is {}.",
                    new Object[] { allocatePath, newVersion , version });
        }
    }

    private void markAsAllocated(final LedgerHandle lh) {
        byte[] data = DLUtils.ledgerId2Bytes(lh.getId());
        Utils.zkSetData(zkc, allocatePath, data, getVersion())
            .addEventListener(new FutureEventListener<ZkVersion>() {
                @Override
                public void onSuccess(ZkVersion version) {
                    // we only issue deleting ledger left from previous allocation when we could allocate first ledger
                    // as zookeeper version could prevent us doing stupid things.
                    deleteLedgerLeftFromPreviousAllocationIfNecessary();
                    setVersion(version);
                    setPhase(Phase.ALLOCATED);
                    // complete the allocation after it is marked as allocated
                    completeAllocation(lh);
                }

                @Override
                public void onFailure(Throwable cause) {
                    setPhase(Phase.ERROR);
                    deleteLedger(lh.getId());
                    LOG.error("Fail mark ledger {} as allocated under {} : ",
                            new Object[] { lh.getId(), allocatePath, cause });
                    // fail the allocation since failed to mark it as allocated
                    failAllocation(cause);
                }
            });
    }

    void deleteLedger(final long ledgerId) {
        final Future<Void> deleteFuture = bkc.deleteLedger(ledgerId, true);
        synchronized (ledgerDeletions) {
            ledgerDeletions.add(deleteFuture);
        }
        deleteFuture.onFailure(new AbstractFunction1<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable cause) {
                LOG.error("Error deleting ledger {} for ledger allocator {}, retrying : ",
                        new Object[] { ledgerId, allocatePath, cause });
                if (!isClosing()) {
                    deleteLedger(ledgerId);
                }
                return BoxedUnit.UNIT;
            }
        }).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                synchronized (ledgerDeletions) {
                    ledgerDeletions.remove(deleteFuture);
                }
                return BoxedUnit.UNIT;
            }
        });
    }

    private synchronized boolean isClosing() {
        return closeFuture != null;
    }

    private Future<Void> closeInternal(boolean cleanup) {
        Promise<Void> closePromise;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closePromise = new Promise<Void>();
            closeFuture = closePromise;
        }
        if (!cleanup) {
            LOG.info("Abort ledger allocator without cleaning up on {}.", allocatePath);
            FutureUtils.setValue(closePromise, null);
            return closePromise;
        }
        cleanupAndClose(closePromise);
        return closePromise;
    }

    private void cleanupAndClose(final Promise<Void> closePromise) {
        LOG.info("Closing ledger allocator on {}.", allocatePath);
        final ZKTransaction txn = new ZKTransaction(zkc);
        // try obtain ledger handle
        tryObtain(txn, new OpListener<LedgerHandle>() {
            @Override
            public void onCommit(LedgerHandle r) {
                // no-op
                complete();
            }

            @Override
            public void onAbort(Throwable t) {
                // no-op
                complete();
            }

            private void complete() {
                FutureUtils.setValue(closePromise, null);
                LOG.info("Closed ledger allocator on {}.", allocatePath);
            }
        }).addEventListener(new FutureEventListener<LedgerHandle>() {
            @Override
            public void onSuccess(LedgerHandle lh) {
                // try obtain succeed
                // if we could obtain the ledger handle, we have the responsibility to close it
                deleteLedger(lh.getId());
                // wait for deletion to be completed
                List<Future<Void>> outstandingDeletions;
                synchronized (ledgerDeletions) {
                    outstandingDeletions = Lists.newArrayList(ledgerDeletions);
                }
                Future.collect(outstandingDeletions).addEventListener(new FutureEventListener<List<Void>>() {
                    @Override
                    public void onSuccess(List<Void> values) {
                        txn.execute();
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        LOG.debug("Fail to obtain the allocated ledger handle when closing the allocator : ", cause);
                        FutureUtils.setValue(closePromise, null);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                LOG.debug("Fail to obtain the allocated ledger handle when closing the allocator : ", cause);
                FutureUtils.setValue(closePromise, null);
            }
        });

    }

    @Override
    public void start() {
        // nop
    }

    @Override
    public Future<Void> asyncClose() {
        return closeInternal(false);
    }

    @Override
    public Future<Void> delete() {
        return closeInternal(true).flatMap(new AbstractFunction1<Void, Future<Void>>() {
            @Override
            public Future<Void> apply(Void value) {
                return Utils.zkDelete(zkc, allocatePath, getVersion());
            }
        });
    }

}
