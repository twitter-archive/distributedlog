package com.twitter.distributedlog.bk;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.zk.DataWithStat;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
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
import java.util.concurrent.atomic.AtomicInteger;

public class LedgerAllocatorPool implements LedgerAllocator {

    static final Logger logger = LoggerFactory.getLogger(LedgerAllocatorPool.class);

    private final DistributedLogConfiguration conf;
    private final BookKeeperClient bkc;
    private final ZooKeeperClient zkc;
    private final String poolPath;
    private final int corePoolSize;

    private final LinkedList<LedgerAllocator> pendingList =
            new LinkedList<LedgerAllocator>();
    private final LinkedList<LedgerAllocator> allocatingList =
            new LinkedList<LedgerAllocator>();
    private final Map<LedgerHandle, LedgerAllocator> obtainMap =
            new HashMap<LedgerHandle, LedgerAllocator>();

    public LedgerAllocatorPool(String poolPath, int corePoolSize,
                               DistributedLogConfiguration conf,
                               ZooKeeperClient zkc,
                               BookKeeperClient bkc) throws IOException {
        this.poolPath = poolPath;
        this.corePoolSize = corePoolSize;
        this.conf = conf;
        this.zkc = zkc;
        this.bkc = bkc;
        this.zkc.addRef();
        this.bkc.addRef();
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

    private void initializePool() throws IOException {
        try {
            List<String> allocators;
            try {
                allocators = zkc.get().getChildren(poolPath, false);
            } catch (KeeperException.NoNodeException e) {
                logger.info("Allocator Pool {} doesn't exist. Creating it.", poolPath);
                ZkUtils.createFullPathOptimistic(zkc.get(), poolPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
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
                             ZooDefs.Ids.OPEN_ACL_UNSAFE,
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
                LedgerAllocator allocator;
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

    private LedgerAllocator newAllocator() throws IOException {
        String allocatePath;
        try {
            allocatePath = zkc.get().create(poolPath + "/A", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            throw new IOException("Failed to create allocator path under " + poolPath + " : ", e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted on creating allocator path under " + poolPath + " : ", e);
        }
        LedgerAllocator allocator = new SimpleLedgerAllocator(allocatePath, conf, zkc, bkc);
        allocator.start();
        return allocator;
    }

    @Override
    public void allocate() throws IOException {
        LedgerAllocator allocator = null;
        boolean newAllocator = false;
        synchronized (this) {
            if (pendingList.isEmpty()) {
                newAllocator = true;
            } else {
                allocator = pendingList.removeFirst();
            }
        }
        if (newAllocator) {
            allocator = newAllocator();
        }
        allocator.allocate();
        synchronized (this) {
            allocatingList.addLast(allocator);
        }
    }

    @Override
    public LedgerHandle tryObtain(Object txn) throws IOException {
        LedgerAllocator allocator;
        synchronized (this) {
            if (allocatingList.isEmpty()) {
                throw new IOException("No ledger allocator available under " + poolPath + ".");
            } else {
                allocator = allocatingList.removeFirst();
            }
        }
        LedgerHandle lh = allocator.tryObtain(txn);
        synchronized (this) {
            obtainMap.put(lh, allocator);
        }
        return lh;
    }

    @Override
    public void confirmObtain(LedgerHandle ledger, Object result) {
        LedgerAllocator allocator;
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
        LedgerAllocator allocator;
        synchronized (this) {
            allocator = obtainMap.remove(ledger);
            if (null == allocator) {
                logger.error("No allocator found for {} under {}.", ledger.getId(), poolPath);
                throw new IllegalArgumentException("No allocator found for ledger " + ledger.getId()
                        + " under " + poolPath + ".");
            }
        }
        allocator.abortObtain(ledger);
        synchronized (this) {
            pendingList.addLast(allocator);
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            for (LedgerAllocator allocator : pendingList) {
                allocator.close();
            }
            for (LedgerAllocator allocator : allocatingList) {
                allocator.close();
            }
            for (LedgerAllocator allocator : obtainMap.values()) {
                allocator.close();
            }
        }
        try {
            this.bkc.release();
        } catch (BKException bke) {
            logger.error("Failed to release bookkeeper client reference : ", bke);
        } catch (InterruptedException ie) {
            logger.error("Interrupted on releasing bookkeeper client reference : ", ie);
        }
        this.zkc.close();
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
        try {
            this.bkc.release();
        } catch (BKException bke) {
            logger.error("Failed to release bookkeeper client reference : ", bke);
        } catch (InterruptedException ie) {
            logger.error("Interrupted on releasing bookkeeper client reference : ", ie);
        }
        this.zkc.close();
    }
}
