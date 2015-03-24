package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import com.twitter.distributedlog.stats.AlertStatsLogger;
import com.twitter.distributedlog.zk.DataWithStat;
import com.twitter.distributedlog.util.PermitLimiter;

import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.distributedlog.util.FutureUtils;

import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.FuturePool;

import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.DistributedLogConstants.ZK_VERSION;

class BKLogPartitionWriteHandler extends BKLogPartitionHandler {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    static Class<? extends BKLogPartitionWriteHandler> WRITER_HANDLER_CLASS = null;
    static final Class[] WRITE_HANDLER_CONSTRUCTOR_ARGS = {
        String.class, String.class, DistributedLogConfiguration.class, URI.class,
        ZooKeeperClientBuilder.class, BookKeeperClientBuilder.class,
        ScheduledExecutorService.class, FuturePool.class, OrderedSafeExecutor.class,
        LedgerAllocator.class, StatsLogger.class, AlertStatsLogger.class, String.class, int.class,
        PermitLimiter.class, FeatureProvider.class
    };


    static BKLogPartitionWriteHandler createBKLogPartitionWriteHandler(String name,
                                                                       String streamIdentifier,
                                                                       DistributedLogConfiguration conf,
                                                                       URI uri,
                                                                       ZooKeeperClientBuilder zkcBuilder,
                                                                       BookKeeperClientBuilder bkcBuilder,
                                                                       ScheduledExecutorService executorService,
                                                                       FuturePool orderedFuturePool,
                                                                       OrderedSafeExecutor lockStateExecutor,
                                                                       LedgerAllocator ledgerAllocator,
                                                                       StatsLogger statsLogger,
                                                                       AlertStatsLogger alertStatsLogger,
                                                                       String clientId,
                                                                       int regionId,
                                                                       PermitLimiter writeLimiter,
                                                                       FeatureProvider featureProvider) throws IOException {
        if (ZK_VERSION.getVersion().equals(DistributedLogConstants.ZK33)) {
            return new BKLogPartitionWriteHandler(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder,
                    executorService, orderedFuturePool, lockStateExecutor, ledgerAllocator,
                    false, statsLogger, alertStatsLogger, clientId, regionId, writeLimiter, featureProvider);
        } else {
            if (null == WRITER_HANDLER_CLASS) {
                try {
                    WRITER_HANDLER_CLASS = (Class<? extends BKLogPartitionWriteHandler>)
                            Class.forName("com.twitter.distributedlog.BKLogPartitionWriteHandlerZK34", true,
                                                         BKLogPartitionWriteHandler.class.getClassLoader());
                    LOG.info("Instantiate writer handler class : {}", WRITER_HANDLER_CLASS);
                } catch (ClassNotFoundException e) {
                    throw new IOException("Can't initialize the writer handler class : ", e);
                }
            }
            // create new instance
            Constructor<? extends BKLogPartitionWriteHandler> constructor;
            try {
                constructor = WRITER_HANDLER_CLASS.getDeclaredConstructor(WRITE_HANDLER_CONSTRUCTOR_ARGS);
            } catch (NoSuchMethodException e) {
                throw new IOException("No constructor found for writer handler class " + WRITER_HANDLER_CLASS + " : ", e);
            }
            Object[] arguments = {
                    name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder,
                    executorService, orderedFuturePool, lockStateExecutor,
                    ledgerAllocator, statsLogger, alertStatsLogger, clientId, regionId, writeLimiter, featureProvider
            };
            try {
                return constructor.newInstance(arguments);
            } catch (InstantiationException e) {
                throw new IOException("Failed to instantiate writer handler : ", e);
            } catch (IllegalAccessException e) {
                throw new IOException("Encountered illegal access when instantiating writer handler : ", e);
            } catch (InvocationTargetException e) {
                Throwable targetException = e.getTargetException();
                if (targetException instanceof IOException) {
                    throw (IOException) targetException;
                } else {
                    throw new IOException("Encountered invocation target exception when instantiating writer handler : ", e);
                }
            }
        }
    }

    private static final int LAYOUT_VERSION = -1;

    protected final DistributedReentrantLock lock;
    protected final DistributedReentrantLock deleteLock;
    protected final OrderedSafeExecutor lockStateExecutor;
    protected final String maxTxIdPath;
    protected final MaxTxId maxTxId;
    protected final AtomicInteger startLogSegmentCount = new AtomicInteger(0);
    protected final int ensembleSize;
    protected final int writeQuorumSize;
    protected final int ackQuorumSize;
    // stub for allocation path. (used by zk34)
    protected final String allocationPath;
    protected final DataWithStat allocationData;
    protected final MaxLedgerSequenceNo maxLedgerSequenceNo;
    protected final boolean sanityCheckTxnId;
    protected final int regionId;
    protected FuturePool orderedFuturePool;
    protected final AtomicReference<IOException> metadataException = new AtomicReference<IOException>(null);
    protected volatile boolean closed = false;
    protected boolean recoverInitiated = false;
    protected final RollingPolicy rollingPolicy;
    protected boolean lockHandler = false;
    protected final PermitLimiter writeLimiter;
    protected final FeatureProvider featureProvider;

    private static int bytesToInt(byte[] b) {
        assert b.length >= 4;
        return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
    }

    private static byte[] intToBytes(int i) {
        return new byte[]{
            (byte) (i >> 24),
            (byte) (i >> 16),
            (byte) (i >> 8),
            (byte) (i)};
    }

    // Stats
    private final OpStatsLogger closeOpStats;
    private final OpStatsLogger openOpStats;
    private final OpStatsLogger recoverOpStats;
    private final OpStatsLogger deleteOpStats;

    static class IgnoreNodeExistsStringCallback implements org.apache.zookeeper.AsyncCallback.StringCallback {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            if (KeeperException.Code.OK.intValue() == rc) {
                LOG.trace("Created path {}.", path);
            } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                LOG.debug("Path {} is already existed.", path);
            } else {
                LOG.error("Failed to create path {} : ", path, KeeperException.create(KeeperException.Code.get(rc)));
            }
        }
    }
    static final IgnoreNodeExistsStringCallback IGNORE_NODE_EXISTS_STRING_CALLBACK = new IgnoreNodeExistsStringCallback();

    static class MultiGetCallback implements org.apache.zookeeper.AsyncCallback.DataCallback {

        final AtomicInteger numPendings;
        final AtomicInteger numFailures;
        final org.apache.zookeeper.AsyncCallback.VoidCallback finalCb;
        final Object finalCtx;

        MultiGetCallback(int numRequests, org.apache.zookeeper.AsyncCallback.VoidCallback finalCb, Object ctx) {
            this.numPendings = new AtomicInteger(numRequests);
            this.numFailures = new AtomicInteger(0);
            this.finalCb = finalCb;
            this.finalCtx = ctx;
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (KeeperException.Code.OK.intValue() == rc) {
                assert(ctx instanceof DataWithStat);
                DataWithStat dataWithStat = (DataWithStat) ctx;
                dataWithStat.setDataWithStat(data, stat);
            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                assert(ctx instanceof DataWithStat);
                DataWithStat dataWithStat = (DataWithStat) ctx;
                dataWithStat.setDataWithStat(null, null);
            } else {
                KeeperException ke = KeeperException.create(KeeperException.Code.get(rc));
                LOG.error("Failed to get data from path {} : ", path, ke);
                numFailures.incrementAndGet();
                finalCb.processResult(rc, path, finalCtx);
            }
            if (numPendings.decrementAndGet() == 0 && numFailures.get() == 0) {
                finalCb.processResult(KeeperException.Code.OK.intValue(), path, finalCtx);
            }
        }
    }

    static final LogSegmentFilter WRITE_HANDLE_FILTER = new LogSegmentFilter() {
        @Override
        public Collection<String> filter(Collection<String> fullList) {
            List<String> result = new ArrayList<String>(fullList.size());
            String lastCompletedLogSegmentName = null;
            long lastLedgerSequenceNumber = -1L;
            for (String s : fullList) {
                if (s.startsWith(DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX)) {
                    result.add(s);
                } else if (s.startsWith(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX)) {
                    String[] parts = s.split("_");
                    try {
                        if (2 == parts.length) {
                            // name: logrecs_<ledger_sequence_number>
                            long ledgerSequenceNumber = Long.parseLong(parts[1]);
                            if (ledgerSequenceNumber > lastLedgerSequenceNumber) {
                                lastLedgerSequenceNumber = ledgerSequenceNumber;
                                lastCompletedLogSegmentName = s;
                            }
                        } else if (6 == parts.length) {
                            // name: logrecs_<start_tx_id>_<end_tx_id>_<ledger_sequence_number>_<ledger_id>_<region_id>
                            long ledgerSequenceNumber = Long.parseLong(parts[3]);
                            if (ledgerSequenceNumber > lastLedgerSequenceNumber) {
                                lastLedgerSequenceNumber = ledgerSequenceNumber;
                                lastCompletedLogSegmentName = s;
                            }
                        } else {
                            // name: logrecs_<start_tx_id>_<end_tx_id> or any unknown names
                            // we don't know the ledger sequence from the name, so add it to the list
                            result.add(s);
                        }
                    } catch (NumberFormatException nfe) {
                        LOG.warn("Unexpected sequence number in log segment {} :", s, nfe);
                        result.add(s);
                    }
                } else {
                    LOG.error("Unknown log segment name : {}", s);
                }
            }
            if (null != lastCompletedLogSegmentName) {
                result.add(lastCompletedLogSegmentName);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Filtered log segments {} from {}.", result, fullList);
            }
            return result;
        }
    };

    abstract class MetadataOp {

        void process() throws IOException {
            if (null != metadataException.get()) {
                throw metadataException.get();
            }
            processOp();
        }

        abstract void processOp() throws IOException;
    }

    /**
     * Construct a Bookkeeper journal manager.
     */
    BKLogPartitionWriteHandler(String name,
                               String streamIdentifier,
                               DistributedLogConfiguration conf,
                               URI uri,
                               ZooKeeperClientBuilder zkcBuilder,
                               BookKeeperClientBuilder bkcBuilder,
                               ScheduledExecutorService executorService,
                               FuturePool orderedFuturePool,
                               OrderedSafeExecutor lockStateExecutor,
                               LedgerAllocator allocator,
                               boolean useAllocator,
                               StatsLogger statsLogger,
                               AlertStatsLogger alertStatsLogger,
                               String clientId,
                               int regionId,
                               PermitLimiter writeLimiter,
                               FeatureProvider featureProvider) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder,
              executorService, statsLogger, alertStatsLogger, null, WRITE_HANDLE_FILTER, clientId);
        this.orderedFuturePool = orderedFuturePool;
        this.lockStateExecutor = lockStateExecutor;
        this.writeLimiter = writeLimiter;
        this.featureProvider = featureProvider;

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

        if (conf.getEncodeRegionIDInVersion()) {
            this.regionId = regionId;
        } else {
            this.regionId = DistributedLogConstants.LOCAL_REGION_ID;
        }
        this.sanityCheckTxnId = conf.getSanityCheckTxnID();

        final boolean ownAllocator = useAllocator && (null == allocator);

        this.maxTxIdPath = partitionRootPath + BKLogPartitionHandler.MAX_TXID_PATH;
        final String lockPath = partitionRootPath + BKLogPartitionHandler.LOCK_PATH;
        allocationPath = partitionRootPath + BKLogPartitionHandler.ALLOCATION_PATH;

        // lockData, readLockData & ledgersData just used for checking whether the path exists or not.
        final DataWithStat maxTxIdData = new DataWithStat();
        allocationData = new DataWithStat();
        final DataWithStat ledgersData = new DataWithStat();

        if (conf.getCreateStreamIfNotExists() || ownAllocator) {
            final ZooKeeper zk;
            try {
                zk = zooKeeperClient.get();
            } catch (InterruptedException e) {
                LOG.error("Failed to initialize zookeeper client : ", e);
                throw new DLInterruptedException("Failed to initialize zookeeper client", e);
            }

            createStreamIfNotExists(partitionRootPath, zk, zooKeeperClient.getDefaultACL(), ownAllocator, allocationData, maxTxIdData, ledgersData);
        } else {
            getLedgersData(ledgersData);
        }

        maxLedgerSequenceNo = new MaxLedgerSequenceNo(ledgersData);

        // Schedule fetching ledgers list in background before we access it.
        // We don't need to watch the ledgers list changes for writer, as it manages ledgers list.
        scheduleGetLedgersTask(false, true);

        // Build the locks
        lock = new DistributedReentrantLock(lockStateExecutor, zooKeeperClient, lockPath,
                            conf.getLockTimeoutMilliSeconds(), getLockClientId(), statsLogger, conf.getZKNumRetries(),
                            conf.getLockReacquireTimeoutMilliSeconds(), conf.getLockOpTimeoutMilliSeconds());
        deleteLock = new DistributedReentrantLock(lockStateExecutor, zooKeeperClient, lockPath,
                            conf.getLockTimeoutMilliSeconds(), getLockClientId(), statsLogger, conf.getZKNumRetries(),
                            conf.getLockReacquireTimeoutMilliSeconds(), conf.getLockOpTimeoutMilliSeconds());
        // Construct the max txn id.
        maxTxId = new MaxTxId(zooKeeperClient, maxTxIdPath, conf.getSanityCheckTxnID(), maxTxIdData);

        // Initialize other parameters.
        setLastLedgerRollingTimeMillis(Utils.nowInMillis());

        // Rolling Policy
        if (conf.getLogSegmentRollingIntervalMinutes() > 0) {
            rollingPolicy = new TimeBasedRollingPolicy(conf);
        } else {
            rollingPolicy = new SizeBasedRollingPolicy(conf);
        }

        // Stats
        StatsLogger segmentsStatsLogger = statsLogger.scope("segments");
        openOpStats = segmentsStatsLogger.getOpStatsLogger("open");
        closeOpStats = segmentsStatsLogger.getOpStatsLogger("close");
        recoverOpStats = segmentsStatsLogger.getOpStatsLogger("recover");
        deleteOpStats = segmentsStatsLogger.getOpStatsLogger("delete");
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
    BKLogPartitionWriteHandler lockHandler() throws LockingException {
        if (lockHandler) {
            return this;
        }
        lock.acquire(DistributedReentrantLock.LockReason.WRITEHANDLER);
        lockHandler = true;
        return this;
    }

    BKLogPartitionWriteHandler unlockHandler() throws LockingException {
        if (lockHandler) {
            lock.release(DistributedReentrantLock.LockReason.WRITEHANDLER);
            lockHandler = false;
        }
        return this;
    }

    static void createStreamIfNotExists(final String partitionRootPath,
                                        final ZooKeeper zk,
                                        final List<ACL> acl,
                                        final boolean ownAllocator,
                                        final DataWithStat allocationData,
                                        final DataWithStat maxTxIdData,
                                        final DataWithStat ledgersData) throws IOException {

        // Note re. persistent lock state initialization: the read lock persistent state (path) is
        // initialized here but only used in the read handler. The reason is its more convenient and
        // less error prone to manage all stream structure in one place.
        final String ledgerPath = partitionRootPath + BKLogPartitionHandler.LEDGERS_PATH;
        final String maxTxIdPath = partitionRootPath + BKLogPartitionHandler.MAX_TXID_PATH;
        final String lockPath = partitionRootPath + BKLogPartitionHandler.LOCK_PATH;
        final String readLockPath = partitionRootPath + BKLogPartitionHandler.READ_LOCK_PATH;
        final String versionPath = partitionRootPath + BKLogPartitionHandler.VERSION_PATH;
        final String allocationPath = partitionRootPath + BKLogPartitionHandler.ALLOCATION_PATH;

        // lockData, readLockData & ledgersData just used for checking whether the path exists or not.
        final DataWithStat lockData = new DataWithStat();
        final DataWithStat readLockData = new DataWithStat();
        final DataWithStat versionData = new DataWithStat();

        final CountDownLatch initializeLatch = new CountDownLatch(1);
        final AtomicReference<IOException> exceptionToThrow = new AtomicReference<IOException>(null);

        class CreatePartitionCallback implements org.apache.zookeeper.AsyncCallback.StringCallback,
                org.apache.zookeeper.AsyncCallback.DataCallback {

            // num of zookeeper paths to get.
            final AtomicInteger numPendings = new AtomicInteger(0);

            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (KeeperException.Code.OK.intValue() == rc ||
                        KeeperException.Code.NODEEXISTS.intValue() == rc) {
                    createAndGetZnodes();
                } else {
                    KeeperException ke = KeeperException.create(KeeperException.Code.get(rc));
                    LOG.error("Failed to create partition root path {} : ", partitionRootPath, ke);
                    exceptionToThrow.set(
                            new ZKException("Failed to create partition root path " + partitionRootPath, ke));
                    initializeLatch.countDown();
                }
            }

            private void createAndGetZnodes() {
                // Send the corresponding create requests in sequence and just wait for the last call.
                // maxtxid path
                zk.create(maxTxIdPath, Long.toString(0).getBytes(UTF_8), acl,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // version path
                zk.create(versionPath, intToBytes(LAYOUT_VERSION), acl,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // ledgers path
                zk.create(ledgerPath, new byte[]{'0'}, acl,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // lock path
                zk.create(lockPath, new byte[0], acl,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // read lock path
                zk.create(readLockPath, new byte[0], acl,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // allocation path
                if (ownAllocator) {
                    zk.create(allocationPath, new byte[0], acl,
                            CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                }
                // send get requests to maxTxId, version, allocation
                numPendings.set(ownAllocator ? 3 : 2);
                zk.getData(maxTxIdPath, false, this, maxTxIdData);
                zk.getData(versionPath, false, this, versionData);
                if (ownAllocator) {
                    zk.getData(allocationPath, false, this, allocationData);
                }
            }

            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    assert(ctx instanceof DataWithStat);
                    DataWithStat dataWithStat = (DataWithStat) ctx;
                    dataWithStat.setDataWithStat(data, stat);
                } else {
                    KeeperException ke = KeeperException.create(KeeperException.Code.get(rc));
                    LOG.error("Failed to get data from path {} : ", path, ke);
                    exceptionToThrow.set(new ZKException("Failed to get data from path " + path, ke));
                }
                if (numPendings.decrementAndGet() == 0) {
                    initializeLatch.countDown();
                }
            }
        }

        class GetDataCallback implements org.apache.zookeeper.AsyncCallback.VoidCallback {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    if ((ownAllocator && allocationData.notExists()) || versionData.notExists() || maxTxIdData.notExists() ||
                        lockData.notExists() || readLockData.notExists() || ledgersData.notExists()) {
                        ZkUtils.asyncCreateFullPathOptimistic(zk, partitionRootPath, new byte[] {'0'},
                                acl, CreateMode.PERSISTENT, new CreatePartitionCallback(), null);
                    } else {
                        initializeLatch.countDown();
                    }
                } else {
                    LOG.error("Failed to get partition data from {}.", partitionRootPath);
                    exceptionToThrow.set(new ZKException("Failed to get partiton data from " + partitionRootPath,
                            KeeperException.Code.get(rc)));
                    initializeLatch.countDown();
                }
            }
        }

        final int NUM_PATHS_TO_CHECK = ownAllocator ? 6 : 5;
        MultiGetCallback getCallback = new MultiGetCallback(NUM_PATHS_TO_CHECK, new GetDataCallback(), null);
        zk.getData(maxTxIdPath, false, getCallback, maxTxIdData);
        zk.getData(versionPath, false, getCallback, versionData);
        if (ownAllocator) {
            zk.getData(allocationPath, false, getCallback, allocationData);
        }
        zk.getData(lockPath, false, getCallback, lockData);
        zk.getData(readLockPath, false, getCallback, readLockData);
        zk.getData(ledgerPath, false, getCallback, ledgersData);

        try {
            initializeLatch.await();
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted when initializing write handler for "
                    + partitionRootPath, e);
        }

        if (null != exceptionToThrow.get()) {
            throw exceptionToThrow.get();
        }

        // Initialize the structures with retreived zookeeper data.
        try {
            // Verify Version
            Preconditions.checkNotNull(versionData.getStat());
            Preconditions.checkNotNull(versionData.getData());
            Preconditions.checkArgument(LAYOUT_VERSION == bytesToInt(versionData.getData()));
            // Verify MaxTxId
            Preconditions.checkNotNull(maxTxIdData.getStat());
            Preconditions.checkNotNull(maxTxIdData.getData());
            // Verify Allocation
            if (ownAllocator) {
                Preconditions.checkNotNull(allocationData.getStat());
                Preconditions.checkNotNull(allocationData.getData());
            }
        } catch (IllegalArgumentException iae) {
            throw new UnexpectedException("Invalid partition " + partitionRootPath, iae);
        }
    }

    protected void getLedgersData(final DataWithStat ledgersData) throws IOException {
        // nop: for zk33 write handler, we don't need this ledgers data
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
    public BKPerStreamLogWriter startLogSegment(long txId) throws IOException {
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
    public BKPerStreamLogWriter startLogSegment(long txId, boolean bestEffort, boolean allowMaxTxID)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            BKPerStreamLogWriter writer = doStartLogSegment(txId, bestEffort, allowMaxTxID);
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

    protected BKPerStreamLogWriter doStartLogSegment(long txId, boolean bestEffort, boolean allowMaxTxID) throws IOException {
        checkLogExists();

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
        boolean wroteInprogressZnode = false;
        LedgerHandle lh = null;
        try {
            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate);

            lh = bookKeeperClient.get().createLedger(ensembleSize, writeQuorumSize, ackQuorumSize,
                BookKeeper.DigestType.CRC32,
                digestpw.getBytes(UTF_8));

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

            String inprogressZnodeName = inprogressZNodeName(lh.getId(), txId, ledgerSeqNo);
            String znodePath = inprogressZNode(lh.getId(), txId, ledgerSeqNo);
            LogSegmentLedgerMetadata l =
                new LogSegmentLedgerMetadata.LogSegmentLedgerMetadataBuilder(znodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), txId)
                    .setLedgerSequenceNo(ledgerSeqNo)
                    .setRegionId(regionId).build();

            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentAfterLedgerCreate);

            /*
             * Write the ledger metadata out to the inprogress ledger znode
             * This can fail if for some reason our write lock has
             * expired (@see DistributedReentrantLock) and another process has managed to
             * create the inprogress znode.
             * In this case, throw an exception. We don't want to continue
             * as this would lead to a split brain situation.
             */
            l.write(zooKeeperClient, znodePath,
                    LogSegmentLedgerMetadata.LogSegmentLedgerMetadataVersion.of(conf.getDLLedgerMetadataLayoutVersion()));
            wroteInprogressZnode = true;
            LOG.debug("Storing MaxTxId in startLogSegment  {} {}", znodePath, txId);

            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentAfterInProgressCreate);

            maxTxId.store(txId);
            addLogSegmentToCache(inprogressZnodeName, l);
            LOG.info("Created inprogress log segment {} for {} : {}",
                     new Object[] { inprogressZnodeName, getFullyQualifiedName(), l });
            return new BKPerStreamLogWriter(getFullyQualifiedName(), inprogressZnodeName,
                    conf, conf.getDLLedgerMetadataLayoutVersion(), lh, lock, txId, ledgerSeqNo,
                    executorService, orderedFuturePool, statsLogger, alertStatsLogger, writeLimiter,
                    featureProvider);
        } catch (Exception e) {
            LOG.error("Exception during StartLogSegment", e);
            if (lh != null) {
                try {
                    long id = lh.getId();
                    lh.close();
                    // If we already wrote inprogress znode, we should not delete the ledger.
                    // There are cases where if the thread was interrupted on the client
                    // while the ZNode was being written, it still may have been written while we
                    // hit an exception. For now we will leak the ledger and leave a warning
                    // With ledger pre-allocation we would have a separate node to track allocation
                    // and would be doing a ZK transaction to move the ledger to the stream , this
                    // leak will automatically be addressed in that change
                    // {@link https://jira.twitter.biz/browse/PUBSUB-1230}
                    if (!wroteInprogressZnode) {
                        LOG.warn("Potentially leaking Ledger with Id {}", id);
                    }
                } catch (Exception e2) {
                    //log & ignore, an IOException will be thrown soon
                    LOG.error("Error closing ledger", e2);
                }
            }

            // If we haven't written an in progress node as yet, lets not fail if this was supposed
            // to be best effort, we can retry this later
            if (bestEffort && !wroteInprogressZnode) {
                return null;
            }

            if (e instanceof InterruptedException) {
                throw new DLInterruptedException("Interrupted zookeeper transaction on starting log segment for " +
                    getFullyQualifiedName(), e);
            } else if (e instanceof KeeperException) {
                throw new ZKException("Encountered zookeeper exception on starting log segment for " +
                    getFullyQualifiedName(), (KeeperException) e);
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Error creating ledger", e);
            }
        }
    }

    boolean shouldStartNewSegment(BKPerStreamLogWriter writer) {
        return rollingPolicy.shouldRollLog(writer, lastLedgerRollingTimeMillis);
    }

    class CompleteOp extends MetadataOp {

        final String inprogressZnodeName;
        final long ledgerSeqNo;
        final long ledgerId;
        final long firstTxId;
        final long lastTxId;
        final int recordCount;
        final long lastEntryId;
        final long lastSlotId;

        CompleteOp(String inprogressZnodeName, long ledgerSeqNo,
                   long ledgerId, long firstTxId, long lastTxId,
                   int recordCount, long lastEntryId, long lastSlotId) {
            this.inprogressZnodeName = inprogressZnodeName;
            this.ledgerSeqNo = ledgerSeqNo;
            this.ledgerId = ledgerId;
            this.firstTxId = firstTxId;
            this.lastTxId = lastTxId;
            this.recordCount = recordCount;
            this.lastEntryId = lastEntryId;
            this.lastSlotId = lastSlotId;
        }

        @Override
        void processOp() throws IOException {
            lock.acquire(DistributedReentrantLock.LockReason.COMPLETEANDCLOSE);
            try {
                completeAndCloseLogSegment(inprogressZnodeName, ledgerSeqNo, ledgerId, firstTxId, lastTxId,
                        recordCount, lastEntryId, lastSlotId, false);
                LOG.info("Recovered {} LastTxId:{}", getFullyQualifiedName(), lastTxId);
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

    class CompleteWriterOp extends MetadataOp {

        final BKPerStreamLogWriter writer;

        CompleteWriterOp(BKPerStreamLogWriter writer) {
            this.writer = writer;
        }

        @Override
        void processOp() throws IOException {
            writer.closeToFinalize();
            // in theory closeToFinalize should throw exception if a stream is in error.
            // just in case, add another checking here to make sure we don't close log segment is a stream is in error.
            if (writer.shouldFailCompleteLogSegment()) {
                throw new IOException("PerStreamLogWriter for " + writer.getFullyQualifiedLogSegment() + " is already in error.");
            }
            completeAndCloseLogSegment(
                    inprogressZNodeName(writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLedgerSequenceNumber()),
                    writer.getLedgerSequenceNumber(), writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLastTxId(),
                    writer.getPositionWithinLogSegment(), writer.getLastDLSN().getEntryId(), writer.getLastDLSN().getSlotId(), true);
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
    public void completeAndCloseLogSegment(BKPerStreamLogWriter writer)
        throws IOException {
        new CompleteWriterOp(writer).process();
    }

    @VisibleForTesting
    void completeAndCloseLogSegment(long ledgerSeqNo, long ledgerId, long firstTxId, long lastTxId, int recordCount)
        throws IOException {
        completeAndCloseLogSegment(inprogressZNodeName(ledgerId, firstTxId, ledgerSeqNo), ledgerSeqNo,
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
    void completeAndCloseLogSegment(String inprogressZnodeName, long ledgerSeqNo,
                                    long ledgerId, long firstTxId, long lastTxId,
                                    int recordCount, long lastEntryId, long lastSlotId, boolean shouldReleaseLock)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            doCompleteAndCloseLogSegment(inprogressZnodeName, ledgerSeqNo,
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
    protected void doCompleteAndCloseLogSegment(String inprogressZnodeName, long ledgerSeqNo,
                                                long ledgerId, long firstTxId, long lastTxId, int recordCount,
                                                long lastEntryId, long lastSlotId, boolean shouldReleaseLock)
            throws IOException {
        checkLogExists();
        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        String inprogressPath = inprogressZNode(inprogressZnodeName);
        try {
            Stat inprogressStat = zooKeeperClient.get().exists(inprogressPath, false);
            if (inprogressStat == null) {
                throw new IOException("Inprogress znode " + inprogressPath
                    + " doesn't exist");
            }
            lock.checkOwnershipAndReacquire(true);
            LogSegmentLedgerMetadata l
                = LogSegmentLedgerMetadata.read(zooKeeperClient, inprogressPath);

            if (l.getLedgerId() != ledgerId) {
                throw new IOException("Active ledger has different ID to inprogress. "
                        + l.getLedgerId() + " found, " + ledgerId + " expected");
            }

            if (l.getFirstTxId() != firstTxId) {
                throw new IOException("Transaction id not as expected, "
                    + l.getFirstTxId() + " found, " + firstTxId + " expected");
            }

            lastLedgerRollingTimeMillis = l.finalizeLedger(lastTxId, conf.getEnableRecordCounts() ? recordCount : 0, lastEntryId, lastSlotId);
            String nameForCompletedLedger = completedLedgerZNodeName(ledgerId, firstTxId, lastTxId, ledgerSeqNo);
            String pathForCompletedLedger = completedLedgerZNode(ledgerId, firstTxId, lastTxId, ledgerSeqNo);
            try {
                l.write(zooKeeperClient, pathForCompletedLedger,
                        LogSegmentLedgerMetadata.LogSegmentLedgerMetadataVersion.of(conf.getDLLedgerMetadataLayoutVersion()));
            } catch (KeeperException.NodeExistsException nee) {
                if (!l.checkEquivalence(zooKeeperClient, pathForCompletedLedger)) {
                    throw new IOException("Node " + pathForCompletedLedger + " already exists"
                        + " but data doesn't match");
                }
            }
            LOG.info("Created {} for completing segment {} for {} : {}",
                     new Object[] { nameForCompletedLedger, ledgerSeqNo, getFullyQualifiedName(), l });

            LOG.debug("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressPath, lastTxId);
            maxTxId.store(lastTxId);

            if (FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_FinalizeLedgerBeforeDelete)) {
                return;
            }

            try {
                zooKeeperClient.get().delete(inprogressPath, inprogressStat.getVersion());
                LOG.info("Deleted {} for completing segment {} for {} : {}",
                        new Object[] { inprogressZnodeName, ledgerSeqNo, getFullyQualifiedName(), l });
            } catch (KeeperException.NoNodeException nne) {
                LOG.warn("No segment {} to delete for completing segment {} for {}",
                         new Object[] { inprogressZnodeName, ledgerSeqNo, getFullyQualifiedName() });
            }

            removeLogSegmentFromCache(inprogressZnodeName);
            addLogSegmentToCache(nameForCompletedLedger, l);
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

    public void recoverIncompleteLogSegments() throws IOException {
        if (recoverInitiated) {
            return;
        }
        FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_RecoverIncompleteLogSegments);
        new RecoverOp().process();
    }

    class RecoverOp extends MetadataOp {

        @Override
        void processOp() throws IOException {
            Stopwatch stopwatch = Stopwatch.createStarted();
            boolean success = false;

            try {
                synchronized (BKLogPartitionWriteHandler.this) {
                    if (recoverInitiated) {
                        return;
                    }
                    doProcessOp();
                    recoverInitiated = true;
                }
                success = true;
            } finally {
                if (success) {
                    recoverOpStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                } else {
                    recoverOpStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                }
            }
        }

        void doProcessOp() throws IOException {
            List<LogSegmentLedgerMetadata> segmentList = getFilteredLedgerList(false, false);
            LOG.info("Initiating Recovery For {} : {}", getFullyQualifiedName(), segmentList);
            // if lastLedgerRollingTimeMillis is not updated, we set it to now.
            synchronized (BKLogPartitionWriteHandler.this) {
                if (lastLedgerRollingTimeMillis < 0) {
                    lastLedgerRollingTimeMillis = Utils.nowInMillis();
                }
            }
            for (LogSegmentLedgerMetadata l : segmentList) {
                if (!l.isInProgress()) {
                    continue;
                }
                new RecoverLogSegmentOp(l).process();
            }
        }

        @Override
        public String toString() {
            return String.format("RecoverOp(%s)", getFullyQualifiedName());
        }
    }

    class RecoverLogSegmentOp extends MetadataOp {

        final LogSegmentLedgerMetadata l;

        RecoverLogSegmentOp(LogSegmentLedgerMetadata l) {
            this.l = l;
        }

        @Override
        void processOp() throws IOException {
            long endTxId = DistributedLogConstants.EMPTY_LEDGER_TX_ID;
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
            } else if (endTxId == DistributedLogConstants.EMPTY_LEDGER_TX_ID) {
                // TODO: Empty ledger - Ideally we should just remove it?
                endTxId = l.getFirstTxId();
            }

            CompleteOp completeOp = new CompleteOp(
                    l.getZNodeName(), l.getLedgerSequenceNumber(),
                    l.getLedgerId(), l.getFirstTxId(), endTxId,
                    recordCount, lastEntryId, lastSlotId);
            completeOp.process();
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
            throw new IOException("deleteLog could not acquire exclusive lock on the partition" + getFullyQualifiedName());
        }

        try {
            purgeAllLogs();
        } finally {
            deleteLock.release(DistributedReentrantLock.LockReason.DELETELOG);
        }

        try {
            lock.close();
            deleteLock.close();
            zooKeeperClient.get().exists(ledgerPath, false);
            zooKeeperClient.get().exists(maxTxIdPath, false);
            if (partitionRootPath.toLowerCase().contains("distributedlog")) {
                ZKUtil.deleteRecursive(zooKeeperClient.get(), partitionRootPath);
            } else {
                LOG.warn("Skip deletion of unrecognized ZK Path {}", partitionRootPath);
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + ledgerPath, ie);
            throw new DLInterruptedException("Interrupted while deleting " + ledgerPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error deleting" + ledgerPath + "entry in zookeeper", ke);
        }
    }

    public void purgeAllLogs()
        throws IOException {
        purgeLogsOlderThanInternal(-1);
    }

    public void purgeLogsOlderThan(long minTxIdToKeep)
        throws IOException {
        assert (minTxIdToKeep > 0);
        purgeLogsOlderThanInternal(minTxIdToKeep);
    }

    com.twitter.util.Future<Boolean> setLogsOlderThanDLSNTruncatedAsync(final DLSN dlsn) {
        return new ExecutorServiceFuturePool(executorService).apply(new ExceptionalFunction0<Boolean>() {
            @Override
            public Boolean applyE() throws Throwable {
                return setLogsOlderThanDLSNTruncated(dlsn);
            }
        });
    }

    boolean setLogsOlderThanDLSNTruncated(final DLSN dlsn) throws IOException {
        if (DLSN.InvalidDLSN == dlsn) {
            return true;
        }
        scheduleGetAllLedgersTaskIfNeeded();
        List<LogSegmentLedgerMetadata> logSegments = getFullLedgerList(false, false);
        LOG.debug("Setting truncation status on logs older than {} from {} for {}",
            new Object[] { dlsn, logSegments, getFullyQualifiedName() });
        List<LogSegmentLedgerMetadata> truncateList = new ArrayList<LogSegmentLedgerMetadata>(logSegments.size());
        LogSegmentLedgerMetadata partialTruncate = null;
        LOG.info("{}: Truncating log segments older than {}", getFullyQualifiedName(), dlsn);
        for (int i = 0; i < logSegments.size() - 1; i++) {
            LogSegmentLedgerMetadata l = logSegments.get(i);
            if (!l.isInProgress()) {
                if (l.getLastDLSN().compareTo(dlsn) < 0) {
                    LOG.debug("{}: Truncating log segment {} ", getFullyQualifiedName(), l);
                    truncateList.add(l);
                } else if (l.getFirstDLSN().compareTo(dlsn) < 0) {
                    // Can be satisfied by at most one segment
                    if (null != partialTruncate) {
                        String logMsg = String.format("Potential metadata inconsistency for stream %s at segment %s", getFullyQualifiedName(), l);
                        LOG.error(logMsg);
                        throw new DLIllegalStateException(logMsg);
                    }
                    LOG.info("{}: Partially truncating log segment {} older than {}.", new Object[] {getFullyQualifiedName(), l, dlsn});
                    partialTruncate = l;
                } else {
                    break;
                }
            }
        }
        setTruncationStatus(truncateList, partialTruncate, dlsn);
        return true;
    }

    Future<List<LogSegmentLedgerMetadata>> purgeLogsOlderThanTimestamp(
            final long minTimestampToKeep, final long sanityCheckThreshold) {
        if (minTimestampToKeep >= Utils.nowInMillis()) {
            return Future.exception(new IllegalArgumentException(
                    "Invalid timestamp " + minTimestampToKeep + " to purge logs for " + getFullyQualifiedName()));
        }
        return asyncGetFullLedgerList(false, false).flatMap(
                new Function<List<LogSegmentLedgerMetadata>, Future<List<LogSegmentLedgerMetadata>>>() {
            @Override
            public Future<List<LogSegmentLedgerMetadata>> apply(List<LogSegmentLedgerMetadata> logSegments) {
                List<LogSegmentLedgerMetadata> purgeList = new ArrayList<LogSegmentLedgerMetadata>(logSegments.size());
                for (int iterator = 0; iterator < (logSegments.size() - 1); iterator++) {
                    LogSegmentLedgerMetadata l = logSegments.get(iterator);
                    // When application explicitly truncates segments; timestamp based purge is
                   // only used to cleanup log segments that have been marked for truncation
                    if ((l.isTruncated() || !conf.getExplicitTruncationByApplication()) &&
                        !l.isInProgress() && (l.getCompletionTime() < minTimestampToKeep)) {
                        // Something went wrong - leave the ledger around for debugging
                        //
                        if (conf.getSanityCheckDeletes() && (l.getCompletionTime() < sanityCheckThreshold)) {
                            LOG.warn("Found a ledger {} older than {} for {}",
                                     new Object[] { l, sanityCheckThreshold, getFullyQualifiedName() });
                        } else {
                            purgeList.add(l);
                        }
                    }
                }
                LOG.info("Deleting log segments older than {} for {} : {}",
                        new Object[] { minTimestampToKeep, getFullyQualifiedName(), purgeList });
                return asyncPurgeLogs(purgeList);
            }
        });
    }

    public void purgeLogsOlderThanInternal(long minTxIdToKeep)
        throws IOException {
        List<LogSegmentLedgerMetadata> ledgerList = getFullLedgerList(true, false);

        // If we are deleting the log we can remove the last entry else we must retain
        // at least one ledger for the stream
        int numEntriesToProcess = ledgerList.size() - 1;

        if (minTxIdToKeep < 0) {
            numEntriesToProcess++;
        }

        for (int iterator = 0; iterator < numEntriesToProcess; iterator++) {
            LogSegmentLedgerMetadata l = ledgerList.get(iterator);
            if ((minTxIdToKeep < 0) ||
                ((l.isTruncated() || !conf.getExplicitTruncationByApplication()) &&
                !l.isInProgress() && (l.getLastTxId() < minTxIdToKeep))) {
                deleteLedgerAndMetadata(l);
            }
        }
    }

    private void setTruncationStatus(final List<LogSegmentLedgerMetadata> truncateList,
                                     LogSegmentLedgerMetadata partialTruncate,
                                     DLSN minActiveDLSN) throws IOException {

        for(LogSegmentLedgerMetadata l : truncateList) {
            if (!l.isTruncated()) {
                MetadataUpdater updater = ZkMetadataUpdater.createMetadataUpdater(conf, zooKeeperClient);
                LogSegmentLedgerMetadata newSegment = updater.setLogSegmentTruncated(l);
                removeLogSegmentFromCache(l.getSegmentName());
                addLogSegmentToCache(newSegment.getSegmentName(), newSegment);
            }
        }

        if ((null != partialTruncate) &&
            (!partialTruncate.isPartiallyTruncated() || (partialTruncate.getMinActiveDLSN().compareTo(minActiveDLSN) < 0))) {
            MetadataUpdater updater = ZkMetadataUpdater.createMetadataUpdater(conf, zooKeeperClient);
            LogSegmentLedgerMetadata newSegment = updater.setLogSegmentPartiallyTruncated(partialTruncate, minActiveDLSN);
            removeLogSegmentFromCache(partialTruncate.getSegmentName());
            addLogSegmentToCache(newSegment.getSegmentName(), newSegment);
        }
    }

    private Future<List<LogSegmentLedgerMetadata>> asyncPurgeLogs(
            final List<LogSegmentLedgerMetadata> logs) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Purging logs for {} : {}", getFullyQualifiedName(), logs);
        }
        return FutureUtils.processList(logs,
                new Function<LogSegmentLedgerMetadata, Future<LogSegmentLedgerMetadata>>() {
            @Override
            public Future<LogSegmentLedgerMetadata> apply(LogSegmentLedgerMetadata segment) {
                return asyncDeleteLedgerAndMetadata(segment);
            }
        }, executorService);
    }

    private Future<LogSegmentLedgerMetadata> asyncDeleteLedgerAndMetadata(
            final LogSegmentLedgerMetadata ledgerMetadata) {
        LOG.info("Deleting ledger {} for {}", ledgerMetadata, getFullyQualifiedName());
        final Promise<LogSegmentLedgerMetadata> promise = new Promise<LogSegmentLedgerMetadata>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        promise.addEventListener(new FutureEventListener<LogSegmentLedgerMetadata>() {
            @Override
            public void onSuccess(LogSegmentLedgerMetadata segment) {
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
                                new Object[] { ledgerMetadata.getLedgerId(), getFullyQualifiedName(), bke });
                        promise.setException(bke);
                        return;
                    }
                    // after the ledger is deleted, we delete the metadata znode
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            asyncDeleteMetadata(ledgerMetadata, promise);
                        }
                    });
                }
            }, null);
        } catch (IOException e) {
            promise.setException(BKException.create(BKException.Code.BookieHandleNotAvailableException));
        }
        return promise;
    }

    private void asyncDeleteMetadata(final LogSegmentLedgerMetadata ledgerMetadata,
                                     final Promise<LogSegmentLedgerMetadata> promise) {
        try {
            zooKeeperClient.get().delete(ledgerMetadata.getZkPath(), -1,
                new org.apache.zookeeper.AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx) {
                        if (KeeperException.Code.NONODE.intValue() == rc) {
                            LOG.error("No log segment {} found for {}.",
                                    ledgerMetadata, getFullyQualifiedName());
                        } else if (KeeperException.Code.OK.intValue() != rc) {
                            LOG.error("Couldn't purge {} for {}: with error {}",
                                    new Object[]{ledgerMetadata, getFullyQualifiedName(),
                                            KeeperException.Code.get(rc)});
                            promise.setException(BKException.create(BKException.Code.ZKException));
                            return;
                        }
                        // purge log segment
                        removeLogSegmentFromCache(ledgerMetadata.getZNodeName());
                        promise.setValue(ledgerMetadata);
                    }
                }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            LOG.error("Encountered zookeeper connection issue when purging {} for {}: ",
                    new Object[]{ledgerMetadata, getFullyQualifiedName(), e});
            promise.setException(BKException.create(BKException.Code.ZKException));
        } catch (InterruptedException e) {
            LOG.error("Interrupted when purging {} for {} : ",
                    new Object[]{ledgerMetadata, getFullyQualifiedName(), e});
            promise.setException(BKException.create(BKException.Code.ZKException));
        }
    }

    private void deleteLedgerAndMetadata(LogSegmentLedgerMetadata ledgerMetadata)
            throws IOException {
        try {
            Await.result(asyncDeleteLedgerAndMetadata(ledgerMetadata));
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Failed to purge " + ledgerMetadata
                        + " for " + getFullyQualifiedName(), e);
            }
        }
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
        // close the zookeeper client & bookkeeper client after closing the lock
        super.close();
    }

    String completedLedgerZNodeName(long ledgerId, long firstTxId, long lastTxId, long ledgerSeqNo) {
        if (DistributedLogConstants.LOGSEGMENT_NAME_VERSION == conf.getLogSegmentNameVersion()) {
            return String.format("%s_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, ledgerSeqNo);
        } else {
            return String.format("%s_%018d_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                    firstTxId, lastTxId);
        }
    }

    /**
     * Get the znode path for a finalize ledger
     */
    String completedLedgerZNode(long ledgerId, long firstTxId, long lastTxId, long ledgerSeqNo) {
        return String.format("%s/%s", ledgerPath,
                completedLedgerZNodeName(ledgerId, firstTxId, lastTxId, ledgerSeqNo));
    }

    /**
     * Get the name of the inprogress znode.
     *
     * @return name of the inprogress znode.
     */
    String inprogressZNodeName(long ledgerId, long firstTxId, long ledgerSeqNo) {
        if (DistributedLogConstants.LOGSEGMENT_NAME_VERSION == conf.getLogSegmentNameVersion()) {
            // Lots of the problems are introduced due to different inprogress names with same ledger sequence number.
            // {@link https://jira.twitter.biz/browse/PUBSUB-1964}
            return String.format("%s_%018d", DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX, ledgerSeqNo);
        } else {
            return DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX + "_" + Long.toString(firstTxId, 16);
        }
    }

    /**
     * Get the znode path for the inprogressZNode
     */
    String inprogressZNode(long ledgerId, long firstTxId, long ledgerSeqNo) {
        return ledgerPath + "/" + inprogressZNodeName(ledgerId, firstTxId, ledgerSeqNo);
    }

    String inprogressZNode(String inprogressZNodeName) {
        return ledgerPath + "/" + inprogressZNodeName;
    }
}
