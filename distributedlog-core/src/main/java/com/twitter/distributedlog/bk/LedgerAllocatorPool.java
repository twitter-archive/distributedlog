package com.twitter.distributedlog.bk;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

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
    private final QuorumConfigProvider quorumConfigProvider;
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
    private final Map<SimpleLedgerAllocator, LedgerHandle> reverseObtainMap =
            new HashMap<SimpleLedgerAllocator, LedgerHandle>();

    public LedgerAllocatorPool(String poolPath, int corePoolSize,
                               DistributedLogConfiguration conf,
                               ZooKeeperClient zkc,
                               BookKeeperClient bkc,
                               ScheduledExecutorService scheduledExecutorService) throws IOException {
        this.poolPath = poolPath;
        this.corePoolSize = corePoolSize;
        this.conf = conf;
        this.quorumConfigProvider =
                new ImmutableQuorumConfigProvider(conf.getQuorumConfig());
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
                Versioned<byte[]> allocatorData =
                        new Versioned<byte[]>(data, new ZkVersion(stat.getVersion()));
                SimpleLedgerAllocator allocator =
                        new SimpleLedgerAllocator(path, allocatorData, quorumConfigProvider, zkc, bkc);
                allocator.start();
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
                        Versioned<byte[]> allocatorData =
                                new Versioned<byte[]>(data, new ZkVersion(stat.getVersion()));
                        logger.info("Rescuing ledger allocator {}.", path);
                        newAllocator = new SimpleLedgerAllocator(path, allocatorData, quorumConfigProvider, zkc, bkc);
                        newAllocator.start();
                        logger.info("Rescued ledger allocator {}.", path);
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
    public Future<LedgerHandle> tryObtain(final Transaction<Object> txn,
                                          final Transaction.OpListener<LedgerHandle> listener) {
        final SimpleLedgerAllocator allocator;
        synchronized (this) {
            if (allocatingList.isEmpty()) {
                return Future.exception(new IOException("No ledger allocator available under " + poolPath + "."));
            } else {
                allocator = allocatingList.removeFirst();
            }
        }

        final Promise<LedgerHandle> tryObtainPromise = new Promise<LedgerHandle>();
        final FutureEventListener<LedgerHandle> tryObtainListener = new FutureEventListener<LedgerHandle>() {
            @Override
            public void onSuccess(LedgerHandle lh) {
                synchronized (LedgerAllocatorPool.this) {
                    obtainMap.put(lh, allocator);
                    reverseObtainMap.put(allocator, lh);
                    tryObtainPromise.setValue(lh);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                try {
                    rescueAllocator(allocator);
                } catch (IOException ioe) {
                    logger.info("Failed to rescue allocator {}", allocator.allocatePath, ioe);
                }
                tryObtainPromise.setException(cause);
            }
        };

        allocator.tryObtain(txn, new Transaction.OpListener<LedgerHandle>() {
            @Override
            public void onCommit(LedgerHandle lh) {
                confirmObtain(allocator);
                listener.onCommit(lh);
            }

            @Override
            public void onAbort(Throwable t) {
                abortObtain(allocator);
                listener.onAbort(t);
            }
        }).addEventListener(tryObtainListener);
        return tryObtainPromise;
    }

    void confirmObtain(SimpleLedgerAllocator allocator) {
        synchronized (this) {
            LedgerHandle lh = reverseObtainMap.remove(allocator);
            if (null != lh) {
                obtainMap.remove(lh);
            }
        }
        synchronized (this) {
            pendingList.addLast(allocator);
        }
    }

    void abortObtain(SimpleLedgerAllocator allocator) {
        synchronized (this) {
            LedgerHandle lh = reverseObtainMap.remove(allocator);
            if (null != lh) {
                obtainMap.remove(lh);
            }
        }
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
    public Future<Void> asyncClose() {
        List<LedgerAllocator> allocatorsToClose;
        synchronized (this) {
            allocatorsToClose = Lists.newArrayListWithExpectedSize(
                    pendingList.size() + allocatingList.size() + obtainMap.size());
            for (LedgerAllocator allocator : pendingList) {
                allocatorsToClose.add(allocator);
            }
            for (LedgerAllocator allocator : allocatingList) {
                allocatorsToClose.add(allocator);
            }
            for (LedgerAllocator allocator : obtainMap.values()) {
                allocatorsToClose.add(allocator);
            }
        }
        return FutureUtils.processList(allocatorsToClose, new Function<LedgerAllocator, Future<Void>>() {
            @Override
            public Future<Void> apply(LedgerAllocator allocator) {
                return allocator.asyncClose();
            }
        }, scheduledExecutorService).map(new AbstractFunction1<List<Void>, Void>() {
            @Override
            public Void apply(List<Void> values) {
                return null;
            }
        });
    }

    @Override
    public Future<Void> delete() {
        List<LedgerAllocator> allocatorsToDelete;
        synchronized (this) {
            allocatorsToDelete = Lists.newArrayListWithExpectedSize(
                    pendingList.size() + allocatingList.size() + obtainMap.size());
            for (LedgerAllocator allocator : pendingList) {
                allocatorsToDelete.add(allocator);
            }
            for (LedgerAllocator allocator : allocatingList) {
                allocatorsToDelete.add(allocator);
            }
            for (LedgerAllocator allocator : obtainMap.values()) {
                allocatorsToDelete.add(allocator);
            }
        }
        return FutureUtils.processList(allocatorsToDelete, new Function<LedgerAllocator, Future<Void>>() {
            @Override
            public Future<Void> apply(LedgerAllocator allocator) {
                return allocator.delete();
            }
        }, scheduledExecutorService).flatMap(new AbstractFunction1<List<Void>, Future<Void>>() {
            @Override
            public Future<Void> apply(List<Void> values) {
                return Utils.zkDelete(zkc, poolPath, new ZkVersion(-1));
            }
        });
    }
}
