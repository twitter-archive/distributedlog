package com.twitter.distributedlog.bk;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.zk.DataWithStat;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LedgerAllocatorPool implements LedgerAllocator {

    static final Logger logger = LoggerFactory.getLogger(LedgerAllocatorPool.class);

    private final DistributedLogConfiguration conf;
    private final BookKeeperClient bkc;
    private final ZooKeeperClient zkc;
    private final ScheduledExecutorService scheduledExecutorService;
    private final String poolPath;
    private final int corePoolSize;

    private final LinkedList<SimpleLedgerAllocator> pendingList =
            new LinkedList<SimpleLedgerAllocator>();
    private final LinkedList<SimpleLedgerAllocator> allocatingList =
            new LinkedList<SimpleLedgerAllocator>();
    private final Map<String, SimpleLedgerAllocator> rescueMap =
            new HashMap<String, SimpleLedgerAllocator>();
    private final Map<LedgerHandle, SimpleLedgerAllocator> obtainMap =
            new HashMap<LedgerHandle, SimpleLedgerAllocator>();

    public LedgerAllocatorPool(String poolPath, int corePoolSize,
                               DistributedLogConfiguration conf,
                               ZooKeeperClient zkc,
                               BookKeeperClient bkc,
                               ScheduledExecutorService scheduledExecutorService) throws IOException {
        this.poolPath = poolPath;
        this.corePoolSize = corePoolSize;
        this.conf = conf;
        this.zkc = zkc;
        this.bkc = bkc;
        this.scheduledExecutorService = scheduledExecutorService;
        initializePool();
    }

    @Override
    public void start() throws IOException {
        for (LedgerAllocator allocator : pendingList) {
            // issue allocating requests during initialize
            allocator.allocate();
        }
    }

    @VisibleForTesting
    synchronized int pendingListSize() {
        return pendingList.size();
    }

    @VisibleForTesting
    synchronized int allocatingListSize() {
        return allocatingList.size();
    }

    @VisibleForTesting
    synchronized int obtainMapSize() {
        return obtainMap.size();
    }

    @VisibleForTesting
    synchronized int rescueSize() {
        return rescueMap.size();
    }

    @VisibleForTesting
    synchronized SimpleLedgerAllocator getLedgerAllocator(LedgerHandle lh) {
        return obtainMap.get(lh);
    }

    private void initializePool() throws IOException {
        try {
            List<String> allocators;
            try {
                allocators = zkc.get().getChildren(poolPath, false);
            } catch (KeeperException.NoNodeException e) {
                logger.info("Allocator Pool {} doesn't exist. Creating it.", poolPath);
                ZkUtils.createFullPathOptimistic(zkc.get(), poolPath, new byte[0], zkc.getDefaultACL(),
                        CreateMode.PERSISTENT);
                allocators = zkc.get().getChildren(poolPath, false);
            }
            if (null == allocators) {
                allocators = new ArrayList<String>();
            }
            if (allocators.size() < corePoolSize) {
                createAllocators(corePoolSize - allocators.size());
                allocators = zkc.get().getChildren(poolPath, false);
            }
            initializeAllocators(allocators);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted when ensuring " + poolPath + " created : ", ie);
        } catch (KeeperException ke) {
            throw new IOException("Encountered zookeeper exception when initializing pool " + poolPath + " : ", ke);
        }
    }

    private void createAllocators(int numAllocators) throws InterruptedException, IOException {
        final AtomicInteger numPendings = new AtomicInteger(numAllocators);
        final AtomicInteger numFailures = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        AsyncCallback.StringCallback createCallback = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (KeeperException.Code.OK.intValue() != rc) {
                    numFailures.incrementAndGet();
                    latch.countDown();
                    return;
                }
                if (numPendings.decrementAndGet() == 0 && numFailures.get() == 0) {
                    latch.countDown();
                }
            }
        };
        for (int i = 0; i < numAllocators; i++) {
            zkc.get().create(poolPath + "/A", new byte[0],
                             zkc.getDefaultACL(),
                             CreateMode.PERSISTENT_SEQUENTIAL,
                             createCallback, null);
        }
        latch.await();
        if (numFailures.get() > 0) {
            throw new IOException("Failed to create " + numAllocators + " allocators.");
        }
    }

    /**
     * Initialize simple allocators with given list of allocator names <i>allocators</i>.
     * It initializes a simple allocator with its simple allocator path.
     */
    private void initializeAllocators(List<String> allocators) throws IOException, InterruptedException {
        final AtomicInteger numPendings = new AtomicInteger(allocators.size());
        final AtomicInteger numFailures = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(numPendings.get() > 0 ? 1 : 0);
        AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() != rc) {
                    numFailures.incrementAndGet();
                    latch.countDown();
                    return;
                }
                DataWithStat dataWithStat = new DataWithStat();
                dataWithStat.setDataWithStat(data, stat);
                SimpleLedgerAllocator allocator;
                try {
                    allocator = new SimpleLedgerAllocator(path, dataWithStat, conf, zkc, bkc);
                    allocator.start();
                } catch (IOException e) {
                    numFailures.incrementAndGet();
                    latch.countDown();
                    return;
                }
                pendingList.add(allocator);
                if (numPendings.decrementAndGet() == 0 && numFailures.get() == 0) {
                    latch.countDown();
                }
            }
        };
        for (String name : allocators) {
            String path = poolPath + "/" + name;
            zkc.get().getData(path, false, dataCallback, null);
        }
        latch.await();
        if (numFailures.get() > 0) {
            throw new IOException("Failed to initialize allocators : " + allocators);
        }
    }

    private void scheduleAllocatorRescue(final SimpleLedgerAllocator ledgerAllocator) {
        try {
            scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        rescueAllocator(ledgerAllocator);
                    } catch (DLInterruptedException dle) {
                        Thread.currentThread().interrupt();
                    }
                }
            }, conf.getZKRetryBackoffStartMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            logger.warn("Failed to schedule rescuing ledger allocator {} : ", ledgerAllocator.allocatePath, ree);
        }
    }

    /**
     * Rescue a ledger allocator from an ERROR state
     * @param ledgerAllocator
     *          ledger allocator to rescue
     */
    private void rescueAllocator(final SimpleLedgerAllocator ledgerAllocator) throws DLInterruptedException {
        SimpleLedgerAllocator oldAllocator;
        synchronized (this) {
            oldAllocator = rescueMap.put(ledgerAllocator.allocatePath, ledgerAllocator);
        }
        if (oldAllocator != null) {
            logger.info("ledger allocator {} is being rescued.", ledgerAllocator.allocatePath);
            return;
        }
        try {
            zkc.get().getData(ledgerAllocator.allocatePath, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    boolean retry = false;
                    SimpleLedgerAllocator newAllocator = null;
                    if (KeeperException.Code.OK.intValue() == rc) {
                        DataWithStat dataWithStat = new DataWithStat();
                        dataWithStat.setDataWithStat(data, stat);
                        try {
                            logger.info("Rescuing ledger allocator {}.", path);
                            newAllocator = new SimpleLedgerAllocator(path, dataWithStat, conf, zkc, bkc);
                            newAllocator.start();
                            logger.info("Rescued ledger allocator {}.", path);
                        } catch (IOException ioe) {
                            logger.warn("Failed to rescue ledger allocator {}, retry rescuing it later : ", path, ioe);
                            retry = true;
                        }
                    } else if (KeeperException.Code.NONODE.intValue() == rc) {
                        logger.info("Ledger allocator {} doesn't exist, skip rescuing it.", path);
                    } else {
                        retry = true;
                    }
                    synchronized (LedgerAllocatorPool.this) {
                        rescueMap.remove(ledgerAllocator.allocatePath);
                        if (null != newAllocator) {
                            pendingList.addLast(newAllocator);
                        }
                    }
                    if (retry) {
                        scheduleAllocatorRescue(ledgerAllocator);
                    }
                }
            }, null);
        } catch (InterruptedException ie) {
            logger.warn("Interrupted on rescuing ledger allocator {} : ", ledgerAllocator.allocatePath, ie);
            synchronized (LedgerAllocatorPool.this) {
                rescueMap.remove(ledgerAllocator.allocatePath);
            }
            throw new DLInterruptedException("Interrupted on rescuing ledger allocator " + ledgerAllocator.allocatePath, ie);
        } catch (IOException ioe) {
            logger.warn("Failed to rescue ledger allocator {}, retry rescuing it later : ", ledgerAllocator.allocatePath, ioe);
            synchronized (LedgerAllocatorPool.this) {
                rescueMap.remove(ledgerAllocator.allocatePath);
            }
            scheduleAllocatorRescue(ledgerAllocator);
        }
    }

    @Override
    public void allocate() throws IOException {
        SimpleLedgerAllocator allocator;
        synchronized (this) {
            if (pendingList.isEmpty()) {
                // if no ledger allocator available, we should fail it immediately, which the request will be redirected to other
                // proxies
                throw new IOException("No ledger allocator available under " + poolPath + ".");
            } else {
                allocator = pendingList.removeFirst();
            }
        }
        boolean success = false;
        try {
            allocator.allocate();
            synchronized (this) {
                allocatingList.addLast(allocator);
            }
            success = true;
        } finally {
            if (!success) {
                rescueAllocator(allocator);
            }
        }
    }

    @Override
    public LedgerHandle tryObtain(Object txn) throws IOException {
        SimpleLedgerAllocator allocator;
        synchronized (this) {
            if (allocatingList.isEmpty()) {
                throw new IOException("No ledger allocator available under " + poolPath + ".");
            } else {
                allocator = allocatingList.removeFirst();
            }
        }
        boolean success = false;
        try {
            LedgerHandle lh = allocator.tryObtain(txn);
            synchronized (this) {
                obtainMap.put(lh, allocator);
            }
            success = true;
            return lh;
        } finally {
            if (!success) {
                rescueAllocator(allocator);
            }
        }
    }

    @Override
    public void confirmObtain(LedgerHandle ledger, Object result) {
        SimpleLedgerAllocator allocator;
        synchronized (this) {
            allocator = obtainMap.remove(ledger);
            if (null == allocator) {
                logger.error("No allocator found for {} under {}.", ledger.getId(), poolPath);
                throw new IllegalArgumentException("No allocator found for ledger " + ledger.getId()
                        + " under " + poolPath + ".");
            }
        }
        allocator.confirmObtain(ledger, result);
        synchronized (this) {
            pendingList.addLast(allocator);
        }
    }

    @Override
    public void abortObtain(LedgerHandle ledger) {
        SimpleLedgerAllocator allocator;
        synchronized (this) {
            allocator = obtainMap.remove(ledger);
            if (null == allocator) {
                logger.error("No allocator found for {} under {}.", ledger.getId(), poolPath);
                throw new IllegalArgumentException("No allocator found for ledger " + ledger.getId()
                        + " under " + poolPath + ".");
            }
        }
        allocator.abortObtain(ledger);
        // if a ledger allocator is aborted, it is better to rescue it. since the ledger allocator might
        // already encounter BadVersion exception.
        try {
            rescueAllocator(allocator);
        } catch (DLInterruptedException e) {
            logger.warn("Interrupted on rescuing ledger allocator pool {} : ", poolPath, e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close(boolean cleanup) {
        synchronized (this) {
            for (LedgerAllocator allocator : pendingList) {
                allocator.close(cleanup);
            }
            for (LedgerAllocator allocator : allocatingList) {
                allocator.close(cleanup);
            }
            for (LedgerAllocator allocator : obtainMap.values()) {
                allocator.close(cleanup);
            }
        }
    }

    @Override
    public void delete() throws IOException {
        synchronized (this) {
            for (LedgerAllocator allocator : pendingList) {
                allocator.delete();
            }
            for (LedgerAllocator allocator : allocatingList) {
                allocator.delete();
            }
            for (LedgerAllocator allocator : obtainMap.values()) {
                allocator.delete();
            }
        }
        try {
            zkc.get().delete(poolPath, -1);
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on deleting allocator pool " + poolPath + " : ", ie);
        } catch (KeeperException ke) {
            throw new IOException("Error on deleting allocator pool " + poolPath + " : ", ke);
        }
    }
}
