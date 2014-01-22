package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.zk.DataWithStat;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static com.twitter.distributedlog.DistributedLogConstants.ZK_VERSION;

class BKLogPartitionWriteHandler extends BKLogPartitionHandler implements AsyncCallback.CloseCallback {
    static final Logger LOG = LoggerFactory.getLogger(BKLogPartitionReadHandler.class);

    static Class<? extends BKLogPartitionWriteHandler> WRITER_HANDLER_CLASS = null;
    static final Class[] WRITE_HANDLER_CONSTRUCTOR_ARGS = {
        String.class, String.class, DistributedLogConfiguration.class, URI.class,
        ZooKeeperClientBuilder.class, BookKeeperClientBuilder.class,
        ScheduledExecutorService.class, ExecutorService.class,
        LedgerAllocator.class, StatsLogger.class, String.class, int.class
    };

    static BKLogPartitionWriteHandler createBKLogPartitionWriteHandler(String name,
                                                                       String streamIdentifier,
                                                                       DistributedLogConfiguration conf,
                                                                       URI uri,
                                                                       ZooKeeperClientBuilder zkcBuilder,
                                                                       BookKeeperClientBuilder bkcBuilder,
                                                                       ScheduledExecutorService executorService,
                                                                       ExecutorService metadataExecutor,
                                                                       LedgerAllocator ledgerAllocator,
                                                                       StatsLogger statsLogger,
                                                                       String clientId,
                                                                       int regionId) throws IOException {
        if (ZK_VERSION.getVersion().equals("3.3")) {
            return new BKLogPartitionWriteHandler(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder,
                    executorService, metadataExecutor, ledgerAllocator, false, statsLogger, clientId, regionId);
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
                    executorService, metadataExecutor, ledgerAllocator, statsLogger, clientId, regionId
            };
            try {
                return constructor.newInstance(arguments);
            } catch (InstantiationException e) {
                throw new IOException("Failed to instantiate writer handler : ", e);
            } catch (IllegalAccessException e) {
                throw new IOException("Encountered illegal access when instantiating writer handler : ", e);
            } catch (InvocationTargetException e) {
                throw new IOException("Encountered invocation target exception when instantiating writer handler : ", e);
            }
        }
    }

    private static final int LAYOUT_VERSION = -1;

    protected final DistributedReentrantLock lock;
    protected final DistributedReentrantLock deleteLock;
    protected final String maxTxIdPath;
    protected final MaxTxId maxTxId;
    protected final AtomicInteger startLogSegmentCount = new AtomicInteger(0);
    protected final int ensembleSize;
    protected final int writeQuorumSize;
    protected final int ackQuorumSize;
    // stub for allocation path. (used by zk34)
    protected final String allocationPath;
    protected final DataWithStat allocationData;
    protected final boolean sanityCheckTxnId;
    protected final int regionId;
    protected final ExecutorService metadataExecutor;
    protected final AtomicReference<IOException> metadataException = new AtomicReference<IOException>(null);
    protected final LinkedHashSet<MetadataOp> pendingMetadataOps = new LinkedHashSet<MetadataOp>();
    protected volatile boolean closed = false;
    protected boolean recoverInitiated = false;

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
    private static OpStatsLogger closeOpStats;
    private static OpStatsLogger openOpStats;
    private static OpStatsLogger recoverOpStats;
    private static OpStatsLogger recoverLastEntryStats;
    private static OpStatsLogger deleteOpStats;

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
            for (String s : fullList) {
                if (s.startsWith(DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX)) {
                    result.add(s);
                } else if (s.startsWith(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX)) {
                    if (lastCompletedLogSegmentName == null || s.compareTo(lastCompletedLogSegmentName) > 0) {
                        lastCompletedLogSegmentName = s;
                    }
                } else {
                    LOG.error("Unknown log segment name : {}", s);
                }
            }
            if (result.isEmpty() && null != lastCompletedLogSegmentName) {
                result.add(lastCompletedLogSegmentName);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Filtered log segments {} from {}.", result, fullList);
            }
            return result;
        }
    };

    abstract class MetadataOp implements Runnable {

        Future<?> future = null;

        void process() throws IOException {
            if (null != metadataException.get()) {
                throw metadataException.get();
            }
            if (null != metadataExecutor) {
                synchronized (pendingMetadataOps) {
                    pendingMetadataOps.add(this);
                }
                if (!closed) {
                    future = metadataExecutor.submit(this);
                }
            } else {
                processOp();
            }
        }

        private void removeOp() {
            synchronized (pendingMetadataOps) {
                pendingMetadataOps.remove(this);
            }
        }

        void waitForCompleted() {
            if (null != future) {
                try {
                    future.get();
                } catch (ExecutionException ee) {
                    LOG.error("Exception on executing {} : ", this, ee);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted on waiting {} to be completed : ", this, e);
                }
            }
        }

        @Override
        public void run() {
            if (null != metadataException.get()) {
                removeOp();
                return;
            }
            try {
                processOp();
            } catch (ZKException zke) {
                if (!closed && ZKException.isRetryableZKException(zke) && null != metadataExecutor) {
                    future = executorService.schedule(new Runnable() {
                        @Override
                        public void run() {
                            metadataExecutor.submit(MetadataOp.this);
                        }
                    }, conf.getZKSessionTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
                    return;
                }
                LOG.error("Error on executing metadata op {} for {} : ",
                        new Object[] { this, getFullyQualifiedName(), zke });
                metadataException.set(zke);
            } catch (IOException ioe) {
                LOG.error("Error on executing metadata op {} for {} : ",
                        new Object[] { this, getFullyQualifiedName(), ioe });
                metadataException.set(ioe);
            }
            removeOp();
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
                               ExecutorService metadataExecutor,
                               LedgerAllocator allocator,
                               boolean useAllocator,
                               StatsLogger statsLogger,
                               String clientId,
                               int regionId) throws IOException {
        super(name, streamIdentifier, conf, uri, zkcBuilder, bkcBuilder,
              executorService, statsLogger, null, WRITE_HANDLE_FILTER);
        this.metadataExecutor = metadataExecutor;

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

        this.maxTxIdPath = partitionRootPath + "/maxtxid";
        final String lockPath = partitionRootPath + "/lock";
        allocationPath = partitionRootPath + "/allocation";

        // lockData & ledgersData just used for checking whether the path exists or not.
        final DataWithStat maxTxIdData = new DataWithStat();
        allocationData = new DataWithStat();

        if (conf.getCreateStreamIfNotExists() || ownAllocator) {

            final ZooKeeper zk;
            try {
                zk = zooKeeperClient.get();
            } catch (InterruptedException e) {
                LOG.error("Failed to initialize zookeeper client : ", e);
                throw new DLInterruptedException("Failed to initialize zookeeper client", e);
            }

            createStreamIfNotExists(partitionRootPath, zk, ownAllocator, allocationData, maxTxIdData);
        }

        // Schedule fetching ledgers list in background before we access it.
        // We don't need to watch the ledgers list changes for writer, as it manages ledgers list.
        scheduleGetLedgersTask(false, true);

        if (clientId.equals(DistributedLogConstants.UNKNOWN_CLIENT_ID)){
            try {
                clientId = InetAddress.getLocalHost().toString();
            } catch(Exception exc) {
                // Best effort
                clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
            }
        }

        // Build the locks
        lock = new DistributedReentrantLock(executorService, zooKeeperClient, lockPath,
                            conf.getLockTimeoutMilliSeconds(), clientId, statsLogger);
        deleteLock = new DistributedReentrantLock(executorService, zooKeeperClient, lockPath,
                            conf.getLockTimeoutMilliSeconds(), clientId, statsLogger);
        // Construct the max txn id.
        maxTxId = new MaxTxId(zooKeeperClient, maxTxIdPath, conf.getSanityCheckTxnID(), maxTxIdData);

        // Initialize other parameters.
        setLastLedgerRollingTimeMillis(Utils.nowInMillis());

        // acquire the lock if we enable recover in background
        if (null != metadataExecutor && conf.getRecoverLogSegmentsInBackground()) {
            lock.acquire("WriteHandlerCreate");
        }

        // Stats
        StatsLogger segmentsStatsLogger = statsLogger.scope("segments");
        if (null == openOpStats) {
            openOpStats = segmentsStatsLogger.getOpStatsLogger("open");
        }
        if (null == closeOpStats) {
            closeOpStats = segmentsStatsLogger.getOpStatsLogger("close");
        }
        if (null == recoverOpStats) {
            recoverOpStats = segmentsStatsLogger.getOpStatsLogger("recover");
        }
        if (null == recoverLastEntryStats) {
            recoverLastEntryStats = segmentsStatsLogger.getOpStatsLogger("recover_last_entry");
        }
        if (null == deleteOpStats) {
            deleteOpStats = segmentsStatsLogger.getOpStatsLogger("delete");
        }
    }

    static void createStreamIfNotExists(final String partitionRootPath,
                                        final ZooKeeper zk, final boolean ownAllocator,
                                        final DataWithStat allocationData,
                                        final DataWithStat maxTxIdData) throws IOException {
        final String ledgerPath = partitionRootPath + "/ledgers";
        final String maxTxIdPath = partitionRootPath + "/maxtxid";
        final String lockPath = partitionRootPath + "/lock";
        final String versionPath = partitionRootPath + "/version";
        final String allocationPath = partitionRootPath + "/allocation";

        // lockData & ledgersData just used for checking whether the path exists or not.
        final DataWithStat lockData = new DataWithStat();
        final DataWithStat ledgersData = new DataWithStat();
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
                zk.create(maxTxIdPath, Long.toString(0).getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // version path
                zk.create(versionPath, intToBytes(LAYOUT_VERSION), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // ledgers path
                zk.create(ledgerPath, new byte[]{'0'}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // lock path
                zk.create(lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT, IGNORE_NODE_EXISTS_STRING_CALLBACK, null);
                // allocation path
                if (ownAllocator) {
                    zk.create(allocationPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
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
                        lockData.notExists() || ledgersData.notExists()) {
                        ZkUtils.createFullPathOptimistic(zk, partitionRootPath, new byte[] {'0'},
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new CreatePartitionCallback(), null);
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

        MultiGetCallback getCallback = new MultiGetCallback(ownAllocator ? 5 : 4,
                new GetDataCallback(), null);
        zk.getData(maxTxIdPath, false, getCallback, maxTxIdData);
        zk.getData(versionPath, false, getCallback, versionData);
        if (ownAllocator) {
            zk.getData(allocationPath, false, getCallback, allocationData);
        }
        zk.getData(lockPath, false, getCallback, lockData);
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

    void checkMetadataException() throws IOException {
        if (null != metadataException.get()) {
            throw metadataException.get();
        }
    }

    void register(Watcher watcher) {
        this.zooKeeperClient.register(watcher);
    }

    @Override
    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
        if (BKException.Code.LedgerClosedException == rc) {
            LOG.debug("Ledger is already closed.");
        } else if (BKException.Code.OK != rc) {
            LOG.error("Error closing ledger ", BKException.create(rc));
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Ledger {} is closed.", lh.getId());
            }
        }
    }

    /**
     * Close ledger handle.
     *
     * @param lh
     *          ledger handle to close.
     */
    protected void closeLedger(LedgerHandle lh) {
        if (null != lh) {
            lh.asyncClose(this, null);
        }
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
     */
    public BKPerStreamLogWriter startLogSegment(long txId) throws IOException {
        long start = MathUtils.nowInNano();
        boolean success = false;
        try {
            BKPerStreamLogWriter writer = doStartLogSegment(txId);
            success = true;
            return writer;
        } finally {
            long elapsed = MathUtils.elapsedMSec(start);
            if (success) {
                openOpStats.registerSuccessfulEvent(elapsed);
            } else {
                openOpStats.registerFailedEvent(elapsed);
            }
        }
    }

    protected BKPerStreamLogWriter doStartLogSegment(long txId) throws IOException {
        checkLogExists();

        if ((txId < 0) ||
            (txId == DistributedLogConstants.MAX_TXID)) {
            throw new IOException("Invalid Transaction Id");
        }

        lock.acquire("StartLogSegment");
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
        boolean writeInprogressZnode = false;
        LedgerHandle lh = null;
        try {
            lh = bookKeeperClient.get().createLedger(ensembleSize, writeQuorumSize, ackQuorumSize,
                BookKeeper.DigestType.CRC32,
                digestpw.getBytes(UTF_8));
            String inprogressZnodeName = inprogressZNodeName(txId);
            String znodePath = inprogressZNode(txId);

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

            LogSegmentLedgerMetadata l = new LogSegmentLedgerMetadata(znodePath,
                    conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), txId, ledgerSeqNo, regionId);

            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentAfterLedgerCreate);

            /*
             * Write the ledger metadata out to the inprogress ledger znode
             * This can fail if for some reason our write lock has
             * expired (@see DistributedExclusiveLock) and another process has managed to
             * create the inprogress znode.
             * In this case, throw an exception. We don't want to continue
             * as this would lead to a split brain situation.
             */
            l.write(zooKeeperClient, znodePath);
            writeInprogressZnode = true;
            LOG.debug("Storing MaxTxId in startLogSegment  {} {}", znodePath, txId);

            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_StartLogSegmentAfterInProgressCreate);

            maxTxId.store(txId);
            addLogSegmentToCache(inprogressZnodeName, l);
            return new BKPerStreamLogWriter(conf, lh, lock, txId, ledgerSeqNo, executorService, statsLogger);
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
                    if (!writeInprogressZnode) {
                        LOG.warn("Potentially leaking Ledger with Id {}", id);
                    }
                } catch (Exception e2) {
                    //log & ignore, an IOException will be thrown soon
                    LOG.error("Error closing ledger", e2);
                }
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

    public boolean shouldStartNewSegment() {

        boolean shouldSwitch = false;

        if (conf.getLogSegmentRollingIntervalMinutes() > 0) {
            shouldSwitch = (Utils.elapsedMSec(lastLedgerRollingTimeMillis) >
                ((long)conf.getLogSegmentRollingIntervalMinutes() * 60 * 1000));
        }
        if (shouldSwitch) {
            LOG.debug("Last Finalize Time: {} elapsed time (MSec): {}", lastLedgerRollingTimeMillis,
                    Utils.elapsedMSec(lastLedgerRollingTimeMillis));
        }
        return shouldSwitch;
    }

    class CompleteOp extends MetadataOp {

        final long ledgerId;
        final long firstTxId;
        final long lastTxId;
        final int recordCount;
        final long lastEntryId;
        final long lastSlotId;

        CompleteOp(long ledgerId, long firstTxId, long lastTxId,
                   int recordCount, long lastEntryId, long lastSlotId) {
            this.ledgerId = ledgerId;
            this.firstTxId = firstTxId;
            this.lastTxId = lastTxId;
            this.recordCount = recordCount;
            this.lastEntryId = lastEntryId;
            this.lastSlotId = lastSlotId;
        }

        @Override
        void processOp() throws IOException {
            lock.acquire("CompleteAndCloseLogSegment");
            try {
                completeAndCloseLogSegment(ledgerId, firstTxId, lastTxId,
                        recordCount, lastEntryId, lastSlotId, false);
                LOG.info("Recovered {} LastTxId:{}", getFullyQualifiedName(), lastTxId);
            } finally {
                lock.release("CompleteAndCloseLogSegment");
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
            completeAndCloseLogSegment(
                    writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLastTxId(),
                    writer.getRecordCount(), writer.getLastDLSN().getEntryId(), writer.getLastDLSN().getSlotId(), true);
        }

        @Override
        public String toString() {
            return String.format("CompleteWriterOp(lid=%d, (%d-%d), count=%d, lastEntry=(%d, %d))",
                    writer.getLedgerHandle().getId(), writer.getStartTxId(), writer.getLastTxId(),
                    writer.getRecordCount(), writer.getLastDLSN().getEntryId(), writer.getLastDLSN().getSlotId());
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
    void completeAndCloseLogSegment(long ledgerId, long firstTxId, long lastTxId, int recordCount)
        throws IOException {
        completeAndCloseLogSegment(ledgerId, firstTxId, lastTxId, recordCount, -1, -1, true);
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
    void completeAndCloseLogSegment(long ledgerId, long firstTxId, long lastTxId,
                                    int recordCount, long lastEntryId, long lastSlotId, boolean shouldReleaseLock)
            throws IOException {
        long start = MathUtils.nowInNano();
        boolean success = false;
        try {
            doCompleteAndCloseLogSegment(ledgerId, firstTxId, lastTxId, recordCount,
                                         lastEntryId, lastSlotId, shouldReleaseLock);
            success = true;
        } finally {
            long elapsed = MathUtils.elapsedMSec(start);
            if (success) {
                closeOpStats.registerSuccessfulEvent(elapsed);
            } else {
                closeOpStats.registerFailedEvent(elapsed);
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
    protected void doCompleteAndCloseLogSegment(long ledgerId, long firstTxId, long lastTxId, int recordCount,
                                                long lastEntryId, long lastSlotId, boolean shouldReleaseLock)
            throws IOException {
        checkLogExists();
        LOG.debug("Completing and Closing Log Segment {} {}", firstTxId, lastTxId);
        String inprogressPath = inprogressZNode(firstTxId);
        String inprogressZnodeName = inprogressZNodeName(firstTxId);
        boolean acquiredLocally = false;
        try {
            Stat inprogressStat = zooKeeperClient.get().exists(inprogressPath, false);
            if (inprogressStat == null) {
                throw new IOException("Inprogress znode " + inprogressPath
                    + " doesn't exist");
            }

            acquiredLocally = lock.checkWriteLock(true);
            LogSegmentLedgerMetadata l
                = LogSegmentLedgerMetadata.read(zooKeeperClient, inprogressPath,
                    conf.getDLLedgerMetadataLayoutVersion());

            if (l.getLedgerId() != ledgerId) {
                throw new IOException("Active ledger has different ID to inprogress. "
                        + l.getLedgerId() + " found, " + ledgerId + " expected");
            }

            if (l.getFirstTxId() != firstTxId) {
                throw new IOException("Transaction id not as expected, "
                    + l.getFirstTxId() + " found, " + firstTxId + " expected");
            }

            lastLedgerRollingTimeMillis = l.finalizeLedger(lastTxId, conf.getEnableRecordCounts() ? recordCount : 0, lastEntryId, lastSlotId);
            String nameForCompletedLedger = completedLedgerZNodeName(firstTxId, lastTxId);

            String pathForCompletedLedger = completedLedgerZNode(firstTxId, lastTxId);
            try {
                l.write(zooKeeperClient, pathForCompletedLedger);
            } catch (KeeperException.NodeExistsException nee) {
                if (!l.checkEquivalence(zooKeeperClient, pathForCompletedLedger)) {
                    throw new IOException("Node " + pathForCompletedLedger + " already exists"
                        + " but data doesn't match");
                }
            }
            LOG.debug("Storing MaxTxId in Finalize Path {} LastTxId {}", inprogressPath, lastTxId);
            maxTxId.store(lastTxId);

            if (FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_FinalizeLedgerBeforeDelete)) {
                return;
            }

            zooKeeperClient.get().delete(inprogressPath, inprogressStat.getVersion());
            removeLogSegmentToCache(inprogressZnodeName);
            addLogSegmentToCache(nameForCompletedLedger, l);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when finalising stream " + partitionRootPath, e);
        } catch (KeeperException.NoNodeException e) {
            throw new IOException("Error when finalising stream " + partitionRootPath, e);
        } catch (KeeperException e) {
            throw new ZKException("Error when finalising stream " + partitionRootPath, e);
        } finally {
            if (acquiredLocally || (shouldReleaseLock && startLogSegmentCount.get() > 0)) {
                lock.release("CompleteAndClose");
                startLogSegmentCount.decrementAndGet();
            }
        }
    }

    public void recoverIncompleteLogSegments() throws IOException {
        if (recoverInitiated) {
            return;
        }
        new RecoverOp().process();
    }

    class RecoverOp extends MetadataOp {

        @Override
        void processOp() throws IOException {
            long start = MathUtils.nowInNano();
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
                long elapsed = MathUtils.elapsedMSec(start);
                if (success) {
                    recoverOpStats.registerSuccessfulEvent(elapsed);
                } else {
                    recoverOpStats.registerFailedEvent(elapsed);
                }
            }
        }

        void doProcessOp() throws IOException {
            LOG.info("Initiating Recovery For {}", getFullyQualifiedName());
            List<LogSegmentLedgerMetadata> segmentList = getFilteredLedgerList(false, false);
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

            Stopwatch stopwatch = new Stopwatch().start();
            boolean recovered = false;
            LogRecordWithDLSN record;
            lock.acquire("RecoverLogSegment");
            try {
                LOG.info("Recovering last record in log segment {} for {}.", l, getFullyQualifiedName());
                record = recoverLastRecordInLedger(l, true, true, true);
                recovered = true;
                LOG.info("Recovered last record in log segment {} for {}.", l, getFullyQualifiedName());
            } finally {
                lock.release("RecoverLogSegment");
                if (recovered) {
                    recoverLastEntryStats.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
                } else {
                    recoverLastEntryStats.registerFailedEvent(stopwatch.stop().elapsedMillis());
                }
            }

            if (null != record) {
                endTxId = record.getTransactionId();
                recordCount = record.getCount();
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

            CompleteOp completeOp = new CompleteOp(l.getLedgerId(), l.getFirstTxId(), endTxId,
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
            deleteLock.acquire("DeleteLog");
        } catch (LockingException lockExc) {
            throw new IOException("deleteLog could not acquire exclusive lock on the partition" + getFullyQualifiedName());
        }

        try {
            purgeAllLogs();
        } finally {
            deleteLock.release("DeleteLog");
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

    void purgeLogsOlderThanDLSN(final DLSN dlsn, BookkeeperInternalCallbacks.GenericCallback<Void> callback) {
        scheduleGetAllLedgersTaskIfNeeded();
        List<LogSegmentLedgerMetadata> logSegments = getCachedFullLedgerList(LogSegmentLedgerMetadata.COMPARATOR);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Purging logs older than {} from {} for {}",
                    new Object[] { dlsn, logSegments, getFullyQualifiedName() });
        }
        List<LogSegmentLedgerMetadata> purgeList = new ArrayList<LogSegmentLedgerMetadata>(logSegments.size());
        if (DLSN.InvalidDLSN == dlsn) {
            callback.operationComplete(BKException.Code.OK, null);
            return;
        }
        for (int i = 0; i < logSegments.size() - 1; i++) {
            LogSegmentLedgerMetadata l = logSegments.get(i);
            if (!l.isInProgress() && l.getLastDLSN().compareTo(dlsn) < 0) {
                LOG.info("Deleting log segment {} older than {}.", l, dlsn);
                purgeList.add(l);
            }
        }
        purgeLogs(purgeList, callback);
    }

    void purgeLogsOlderThanTimestamp(final long minTimestampToKeep, final long sanityCheckThreshold,
                                     final BookkeeperInternalCallbacks.GenericCallback<Void> callback) {
        assert (minTimestampToKeep < Utils.nowInMillis());
        scheduleGetAllLedgersTaskIfNeeded();
        List<LogSegmentLedgerMetadata> logSegments = getCachedFullLedgerList(LogSegmentLedgerMetadata.COMPARATOR);
        final List<LogSegmentLedgerMetadata> purgeList = new ArrayList<LogSegmentLedgerMetadata>(logSegments.size());
        boolean logTimestamp = true;
        for (int iterator = 0; iterator < (logSegments.size() - 1); iterator++) {
            LogSegmentLedgerMetadata l = logSegments.get(iterator);
            if ((!l.isInProgress() && l.getCompletionTime() < minTimestampToKeep)) {
                if (logTimestamp) {
                    LOG.info("Deleting ledgers older than {}", minTimestampToKeep);
                    logTimestamp = false;
                }

                // Something went wrong - leave the ledger around for debugging
                //
                if (conf.getSanityCheckDeletes() && (l.getCompletionTime() < sanityCheckThreshold)) {
                    LOG.warn("Found a ledger {} older than {}", l, sanityCheckThreshold);
                } else {
                    purgeList.add(l);
                }
            }
        }
        // purge logs
        purgeLogs(purgeList, callback);
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
                (!l.isInProgress() && l.getLastTxId() < minTxIdToKeep)) {
                deleteLedgerAndMetadata(l);
            }
        }
    }

    private void purgeLogs(final List<LogSegmentLedgerMetadata> logs,
                           final BookkeeperInternalCallbacks.GenericCallback<Void> callback) {
        if (logs.size() == 0) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Nothing to purge.");
            }
            callback.operationComplete(BKException.Code.OK, null);
            return;
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Purging logs : {}.", logs);
        }
        final AtomicInteger numLogs = new AtomicInteger(logs.size());
        final BookkeeperInternalCallbacks.GenericCallback<Void> deleteCallback =
                new BookkeeperInternalCallbacks.GenericCallback<Void>() {
                    @Override
                    public void operationComplete(int rc, Void result) {
                        // we don't really care about the delete result right now
                        if (numLogs.decrementAndGet() == 0) {
                            callback.operationComplete(BKException.Code.OK, null);
                        }
                    }
                };
        for (LogSegmentLedgerMetadata l : logs) {
            deleteLedgerAndMetadata(l, deleteCallback);
        }
    }

    private void deleteLedgerAndMetadata(final LogSegmentLedgerMetadata ledgerMetadata,
                                         final BookkeeperInternalCallbacks.GenericCallback<Void> callback) {
        LOG.info("Deleting ledger for {}", ledgerMetadata);
        try {
            bookKeeperClient.get().asyncDeleteLedger(ledgerMetadata.getLedgerId(), new AsyncCallback.DeleteCallback() {
                @Override
                public void deleteComplete(int rc, Object ctx) {
                    if (BKException.Code.NoSuchLedgerExistsException == rc) {
                        LOG.warn("No ledger {} found to delete for {}.", ledgerMetadata.getLedgerId(), ledgerMetadata);
                        callback.operationComplete(rc, null);
                    } else if (BKException.Code.OK != rc) {
                        LOG.error("Couldn't delete ledger {} from bookkeeper : ",
                                ledgerMetadata.getLedgerId(), BKException.create(rc));
                        callback.operationComplete(rc, null);
                        return;
                    }
                    // after the ledger is deleted, we delete the metadata znode
                    try {
                        zooKeeperClient.get().delete(ledgerMetadata.getZkPath(), -1,
                            new org.apache.zookeeper.AsyncCallback.VoidCallback() {
                                @Override
                                public void processResult(int rc, String path, Object ctx) {
                                    if (KeeperException.Code.OK.intValue() != rc) {
                                        LOG.error("Couldn't purge {} : ", ledgerMetadata,
                                                KeeperException.create(KeeperException.Code.get(rc)));
                                        callback.operationComplete(BKException.Code.ZKException, null);
                                        return;
                                    }
                                    // purge log segment
                                    removeLogSegmentToCache(completedLedgerZNodeName(ledgerMetadata.getFirstTxId(),
                                            ledgerMetadata.getLastTxId()));
                                    callback.operationComplete(BKException.Code.OK, null);
                                }
                            }, null);
                    } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
                        LOG.error("Encountered zookeeper connection issue when purging {} : ", ledgerMetadata, e);
                        callback.operationComplete(BKException.Code.ZKException, null);
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted when purging {}.", ledgerMetadata);
                        callback.operationComplete(BKException.Code.InterruptedException, null);
                    }
                }
            }, null);
        } catch (IOException e) {
            callback.operationComplete(BKException.Code.BookieHandleNotAvailableException, null);
        }
    }

    private void deleteLedgerAndMetadata(LogSegmentLedgerMetadata ledgerMetadata) throws IOException {
        long start = MathUtils.nowInNano();
        boolean success = false;
        try {
            doDeleteLedgerAndMetadata(ledgerMetadata);
            success = true;
        } finally {
            long elapsed = MathUtils.elapsedMSec(start);
            if (success) {
                deleteOpStats.registerSuccessfulEvent(elapsed);
            } else {
                deleteOpStats.registerFailedEvent(elapsed);
            }
        }
    }

    private void doDeleteLedgerAndMetadata(LogSegmentLedgerMetadata ledgerMetadata) {
        final AtomicInteger rcHolder = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        deleteLedgerAndMetadata(ledgerMetadata, new BookkeeperInternalCallbacks.GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                rcHolder.set(rc);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while purging {}", ledgerMetadata);
        }
        if (BKException.Code.OK != rcHolder.get()) {
            LOG.error("Failed to purge {} : ", ledgerMetadata,
                    BKException.create(rcHolder.get()));
        }
    }

    public void close() throws IOException {
        closed = true;
        List<MetadataOp> pendingOps;
        synchronized (pendingMetadataOps) {
            pendingOps = new ArrayList<MetadataOp>(pendingMetadataOps.size());
            pendingOps.addAll(pendingMetadataOps);
        }
        for (MetadataOp op : pendingOps) {
            op.waitForCompleted();
        }
        if (startLogSegmentCount.get() > 0) {
            try {
                while (startLogSegmentCount.decrementAndGet() >= 0) {
                    lock.release("WriteHandlerClose");
                }
            } catch (IOException ioe) {
                LOG.error("Error on releasing WriteHandlerClose {} : ", getFullyQualifiedName(), ioe);
            }
        }
        if (null != metadataExecutor && conf.getRecoverLogSegmentsInBackground()) {
            lock.release("WriteHandlerClose");
        }
        lock.close();
        deleteLock.close();
        // close the zookeeper client & bookkeeper client after closing the lock
        super.close();
    }

    String completedLedgerZNodeName(long startTxId, long endTxId) {
        return String.format("%s_%018d_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                startTxId, endTxId);
    }

    /**
     * Get the znode path for a finalize ledger
     */
    String completedLedgerZNode(long startTxId, long endTxId) {
        return String.format("%s/%s", ledgerPath, completedLedgerZNodeName(startTxId, endTxId));
    }

    /**
     * Get the name of the inprogress znode.
     *
     * @param startTxid
     *          start txn id.
     * @return name of the inprogress znode.
     */
    String inprogressZNodeName(long startTxid) {
        return DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX + "_" + Long.toString(startTxid, 16);
    }

    /**
     * Get the znode path for the inprogressZNode
     */
    String inprogressZNode(long startTxid) {
        return ledgerPath + "/" + inprogressZNodeName(startTxid);
    }
}
