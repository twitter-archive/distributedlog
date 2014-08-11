package com.twitter.distributedlog.bk;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.zk.DataWithStat;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Allocator to allocate ledgers.
 */
public class SimpleLedgerAllocator implements LedgerAllocator, AsyncCallback.CreateCallback {

    static final Logger LOG = LoggerFactory.getLogger(SimpleLedgerAllocator.class);

    static enum Phase {
        ALLOCATING, ALLOCATED, HANDING_OVER, HANDED_OVER, ERROR
    }

    // zookeeper client
    final ZooKeeperClient zkc;
    // bookkeeper client
    final BookKeeperClient bkc;
    // znode path
    final String allocatePath;
    // allocation phase
    Phase phase = Phase.HANDED_OVER;
    // znode stat
    int zkVersion = -1;
    // ledger id.
    LedgerHandle allocated;
    // ledger id left from previous allocation
    Long ledgerIdLeftFromPrevAllocation = null;

    boolean closing = false;
    final AtomicInteger deletions = new AtomicInteger(0);

    // Ledger configuration
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private final byte[] digestpw;

    static DataWithStat createAndGetAllocationData(String allocatePath, ZooKeeperClient zkc) throws IOException {
        try {
            try {
                zkc.get().create(allocatePath, new byte[0], zkc.getDefaultACL(), CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // if node already exists
                LOG.debug("Allocation path {} is already existed.", allocatePath);
            }
            Stat stat = new Stat();
            byte[] data = zkc.get().getData(allocatePath, false, stat);
            DataWithStat dataWithStat = new DataWithStat();
            dataWithStat.setDataWithStat(data, stat);
            return dataWithStat;
        } catch (KeeperException e) {
            throw new IOException("Failed to get allocation data from " + allocatePath, e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when accessing zookeeper client : ", e);
        }
    }

    // Construct the ledger allocator for test.
    SimpleLedgerAllocator(String allocatePath, DistributedLogConfiguration conf,
                          ZooKeeperClient zkc, BookKeeperClient bkc) throws IOException {
        this(allocatePath, createAndGetAllocationData(allocatePath, zkc), conf, zkc, bkc);
    }

    /**
     * Construct a ledger allocator.
     *
     * @param allocatePath
     *          znode path to store the allocated ledger.
     * @param allocationData
     *          allocation data.
     * @param conf
     *          DistributedLog configuration.
     * @param zkc
     *          zookeeper client.
     * @param bkc
     *          bookkeeper client.
     * @throws IOException
     */
    public SimpleLedgerAllocator(String allocatePath, DataWithStat allocationData,
                                 DistributedLogConfiguration conf,
                                 ZooKeeperClient zkc, BookKeeperClient bkc) throws IOException {
        this.zkc = zkc;
        this.bkc = bkc;
        this.allocatePath = allocatePath;
        this.ensembleSize = conf.getEnsembleSize();
        if (this.ensembleSize < conf.getWriteQuorumSize()) {
            this.writeQuorumSize = this.ensembleSize;
            LOG.warn("Setting write quorum size {} greater than ensemble size {}",
                conf.getWriteQuorumSize(), this.ensembleSize);
        } else {
            this.writeQuorumSize = conf.getWriteQuorumSize();
        }
        if (this.writeQuorumSize < conf.getAckQuorumSize()) {
            this.ackQuorumSize = this.writeQuorumSize;
            LOG.warn("Setting write ack quorum size {} greater than write quorum size {}",
                conf.getAckQuorumSize(), this.writeQuorumSize);
        } else {
            this.ackQuorumSize = conf.getAckQuorumSize();
        }
        this.digestpw = conf.getBKDigestPW().getBytes(UTF_8);
        initialize(allocationData);
    }

    /**
     * Start the allocator.
     *
     * @param allocationData
     *          Allocation Data.
     * @throws IOException
     */
    private void initialize(DataWithStat allocationData) throws IOException {
        setZkVersion(allocationData.getStat().getVersion());
        byte[] data = allocationData.getData();
        if (null != data && data.length > 0) {
            // delete the allocated ledger since this is left by last allocation.
            try {
                ledgerIdLeftFromPrevAllocation = bytes2LedgerId(data);
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
            throw new IOException("Error on ledger allocator for " + allocatePath);
        }
        if (Phase.HANDED_OVER == phase) {
            // issue an allocate request when ledger is already handed over.
            allocateLedger();
        }
    }

    /**
     * Try obtaining ledger in given <i>txn</i>.
     */
    @Override
    public synchronized LedgerHandle tryObtain(Object t) throws IOException {
        assert (t instanceof Transaction);
        Transaction txn = (Transaction) t;
        tryObtainLedgerHandle();
        txn.setData(allocatePath, new byte[0], zkVersion);
        setPhase(Phase.HANDING_OVER);
        return allocated;
    }

    private synchronized LedgerHandle tryObtainLedgerHandle() throws IOException {
        if (Phase.ERROR == phase) {
            throw new IOException("Error on allocating ledger under " + allocatePath);
        }
        if (Phase.HANDING_OVER == phase || Phase.HANDED_OVER == phase) {
            throw new IOException("Ledger handle is handling over to another thread : " + phase);
        }
        while (Phase.ALLOCATING == phase) {
            try {
                wait(500);
            } catch (InterruptedException e) {
                // nop
            }
        }
        if (Phase.ALLOCATED != phase) {
            throw new IOException("Failed on allocating ledger on " + allocatePath);
        }
        return allocated;
    }

    /**
     * Confirm obtaining the ledger.
     */
    @Override
    public void confirmObtain(LedgerHandle ledger, Object r) {
        assert (r instanceof OpResult);
        OpResult result = (OpResult) r;
        Preconditions.checkArgument(allocated == ledger);
        Preconditions.checkArgument(result instanceof OpResult.SetDataResult);
        OpResult.SetDataResult setDataResult = (OpResult.SetDataResult) result;
        confirmObtain(setDataResult.getStat().getVersion());
    }

    private void confirmObtain(int zkVersion) {
        boolean shouldAllocate = false;
        synchronized (this) {
            if (Phase.HANDING_OVER == phase) {
                setPhase(Phase.HANDED_OVER);
                allocated = null;
                setZkVersion(zkVersion);
                shouldAllocate = true;
            }
        }
        if (shouldAllocate) {
            // issue an allocation request
            allocateLedger();
        }
    }

    /**
     * Abort the obtain
     */
    @Override
    public synchronized void abortObtain(LedgerHandle ledger) {
        Preconditions.checkArgument(allocated == ledger);
        if (Phase.HANDING_OVER == phase) {
            setPhase(Phase.ALLOCATED);
        }
    }

    private synchronized void setPhase(Phase phase) {
        this.phase = phase;
        LOG.info("Ledger allocator {} moved to phase {} : zk version = {}.",
                new Object[] { allocatePath, phase, zkVersion });
    }

    private synchronized void setPhaseAndNotify(Phase phase) {
        this.phase = phase;
        LOG.info("Ledger allocator {} moved to phase {} and notify waiters : zk version = {}.",
                new Object[] { allocatePath, phase, zkVersion });
        notifyAll();
    }

    private static byte[] ledgerId2Bytes(long ledgerId) {
        return Long.toString(ledgerId).getBytes(UTF_8);
    }

    private static long bytes2LedgerId(byte[] data) {
        return Long.valueOf(new String(data, UTF_8));
    }

    @Override
    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
        if (BKException.Code.OK != rc || null == lh) {
            LOG.error("Error creating ledger for allocating {} : ", allocatePath,
                    BKException.create(rc));
            setPhase(Phase.ERROR);
            return;
        }
        // create a ledger, update the ledger to allocation path before handling it over for usage.
        try {
            markAsAllocated(lh);
        } catch (InterruptedException e) {
            LOG.error("Interrupted when marking ledger {} as allocated : ", lh.getId(), e);
            setPhase(Phase.ERROR);
            deleteLedger(lh.getId());
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            LOG.error("Encountered zookeeper connection issue when marking ledger {} as allocated : ",
                    lh.getId(), e);
            setPhase(Phase.ERROR);
            deleteLedger(lh.getId());
        }
    }

    private synchronized void allocateLedger() {
        // make sure previous allocation is already handed over.
        if (Phase.HANDED_OVER != phase) {
            LOG.error("Trying allocate ledger for {} in phase {}, giving up.", allocatePath, phase);
            return;
        }
        setPhase(Phase.ALLOCATING);
        try {
            bkc.get().asyncCreateLedger(ensembleSize, writeQuorumSize, ackQuorumSize,
                    BookKeeper.DigestType.CRC32, digestpw, this, null);
        } catch (IOException ie) {
            LOG.error("Error allocating ledger for {} : ", allocatePath, ie);
            setPhase(Phase.ERROR);
        }
    }

    private synchronized void markLedgerHandleAsAvailable(final LedgerHandle lh, int zkVersion) {
        this.allocated = lh;
        setZkVersion(zkVersion);
        setPhaseAndNotify(Phase.ALLOCATED);
    }

    private synchronized int getZkVersion() {
        return zkVersion;
    }

    private synchronized void setZkVersion(int newZkVersion) {
        if (newZkVersion > zkVersion) {
            LOG.info("Ledger allocator for {} moved version from {} to {}.",
                    new Object[] { allocatePath, zkVersion, newZkVersion });
            zkVersion = newZkVersion;
        } else {
            LOG.warn("Ledger allocator for {} received an old version {}, current version is {}.",
                    new Object[] { allocatePath, newZkVersion, zkVersion });
        }
    }

    private void markAsAllocated(final LedgerHandle lh)
            throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException {
        byte[] data = ledgerId2Bytes(lh.getId());
        zkc.get().setData(allocatePath, data, getZkVersion(), new org.apache.zookeeper.AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (KeeperException.Code.OK.intValue() != rc) {
                    setPhaseAndNotify(Phase.ERROR);
                    deleteLedger(lh.getId());
                    LOG.error("Fail mark ledger {} as allocated under {} : ",
                            new Object[] { lh.getId(), allocatePath,
                                KeeperException.create(KeeperException.Code.get(rc), path) });
                    return;
                }
                // we only issue deleting ledger left from previous allocation when we could allocate first ledger
                // as zookeeper version could prevent us doing stupid things.
                deleteLedgerLeftFromPreviousAllocationIfNecessary();
                // the ledger is marked as allocated
                markLedgerHandleAsAvailable(lh, stat.getVersion());
            }
        }, null);
    }

    private void deleteLedger(final long ledgerId) {
        deletions.incrementAndGet();

        try {
            LOG.info("Deleting allocated ledger {} from allocator {}.", ledgerId, allocatePath);
            bkc.get().asyncDeleteLedger(ledgerId, new AsyncCallback.DeleteCallback() {
                @Override
                public void deleteComplete(int rc, Object ctx) {
                    deletions.decrementAndGet();
                    if (BKException.Code.OK != rc && BKException.Code.NoSuchLedgerExistsException != rc) {
                        LOG.error("Error deleting ledger {} for ledger allocator {}, retrying : ",
                                new Object[] { ledgerId, allocatePath, BKException.create(rc) });
                        if (!isClosing()) {
                            deleteLedger(ledgerId);
                        }
                    }
                }
            }, null);
        } catch (IOException ie) {
            LOG.error("Error deleting ledger {} for ledger allocator {} : ",
                    new Object[] { ledgerId, allocatePath, ie });
            setPhase(Phase.ERROR);
        }
    }

    private synchronized boolean isClosing() {
        return closing;
    }

    private void closeInternal(boolean cleanup) throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException {
        synchronized (this) {
            if (closing) {
                return;
            }
            closing = true;
        }
        if (!cleanup) {
            LOG.info("Abort ledger allocator without cleaning up on {}.", allocatePath);
            return;
        }
        LOG.info("Closing ledger allocator on {}.", allocatePath);
        // wait for ongoing allocation to be completed
        Transaction txn = zkc.get().transaction();
        try {
            LedgerHandle lh = tryObtain(txn);
            // if we could obtain the ledger handle, we have the responsibility to close it
            deleteLedger(lh.getId());
            // wait for deletion to be completed
            while (0 != deletions.get()) {
                Thread.sleep(1000);
            }
            try {
                // commit the obtain
                List<OpResult> results = txn.commit();
                confirmObtain(lh, results.get(0));
            } catch (KeeperException e) {
                abortObtain(lh);
            }
            LOG.info("Closed ledger allocator on {}.", allocatePath);
        } catch (IOException ie) {
            LOG.debug("Fail to obtain the allocated ledger handle when closing the allocator : ", ie);
            // wait for deletion to be completed
            while (0 != deletions.get()) {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void start() {
        // nop
    }

    @Override
    public void close(boolean cleanup) {
        try {
            closeInternal(cleanup);
        } catch (InterruptedException e) {
            LOG.error("Interrupted releasing ledger allocator {} : ", allocatePath, e);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            LOG.error("ZooKeeper connection exception when closing allocator {} : ", allocatePath, e);
        }
    }

    @Override
    public void delete() throws IOException {
        try {
            closeInternal(true);
            try {
                this.zkc.get().delete(allocatePath, getZkVersion());
                LOG.info("Deleted allocator {}.", allocatePath);
            } catch (KeeperException.NoNodeException nne) {
                LOG.warn("No allocator available in {}.", allocatePath);
            }
        } catch (KeeperException ke) {
            throw new IOException("Exception on deleting allocator " + allocatePath + " : ", ke);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted deleting allocator " + allocatePath, ie);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zce) {
            throw new ZKException("Encountered zookeeper connection issue on deleting allocator " + allocatePath + " : ",
                    KeeperException.Code.CONNECTIONLOSS);
        }
    }

}
