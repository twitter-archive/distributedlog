package com.twitter.distributedlog;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.zk.DataWithStat;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class BKDistributedLogManager extends ZKMetadataAccessor implements DistributedLogManager {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogManager.class);

    static String getPartitionPath(URI uri, String streamName, String streamIdentifier) {
        return String.format("%s/%s/%s", uri.getPath(), streamName, streamIdentifier);
    }

    static void createUnpartitionedStream(ZooKeeper zk, URI uri, String streamName) throws IOException {
        BKLogPartitionWriteHandler.createStreamIfNotExists(getPartitionPath(uri, streamName,
                DistributedLogConstants.DEFAULT_STREAM), zk, true, new DataWithStat(), new DataWithStat());
    }

    private String clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
    private int regionId = DistributedLogConstants.LOCAL_REGION_ID;
    private final DistributedLogConfiguration conf;
    private boolean closed = true;
    private final ScheduledExecutorService executorService;
    private boolean ownExecutor;
    private final BookKeeperClientBuilder bookKeeperClientBuilder;
    private final BookKeeperClient bookKeeperClient;
    private final StatsLogger statsLogger;
    private LedgerAllocator ledgerAllocator = null;
    // Log Segment Rolling Manager to control rolling speed
    private PermitManager logSegmentRollingPermitManager = PermitManager.UNLIMITED_PERMIT_MANAGER;
    // read handler for listener.
    private BKLogPartitionReadHandler readHandlerForListener = null;
    private ExecutorServiceFuturePool orderedFuturePool = null;
    private ExecutorServiceFuturePool readerFuturePool = null;

    private static StatsLogger handlerStatsLogger = null;
    private static OpStatsLogger createWriteHandlerStats = null;

    public BKDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                   ZooKeeperClientBuilder zkcBuilder, BookKeeperClientBuilder bkcBuilder,
                                   StatsLogger statsLogger) throws IOException {
        this(name, conf, uri, zkcBuilder, bkcBuilder,
            Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("BKDL-" + name + "-executor-%d").build()),
            null, statsLogger);
        this.ownExecutor = true;
    }

    public BKDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                   ZooKeeperClientBuilder zkcBuilder,
                                   BookKeeperClientBuilder bkcBuilder,
                                   ScheduledExecutorService executorService,
                                   ClientSocketChannelFactory channelFactory,
                                   StatsLogger statsLogger) throws IOException {
        super(name, conf, uri, zkcBuilder);
        this.conf = conf;
        this.executorService = executorService;
        this.statsLogger = statsLogger;
        this.ownExecutor = false;

        try {
            // Distributed Log Manager always creates a zookeeper connection to
            // handle session expiration
            // Bookkeeper client is only created if separate BK clients option is
            // not specified
            // ZK client should be initialized in the super class
            if (null == bkcBuilder) {
                // resolve uri
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zooKeeperClient, uri);
                BKDLConfig.propagateConfiguration(bkdlConfig, conf);
                this.bookKeeperClientBuilder = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(conf).bkdlConfig(bkdlConfig).name(String.format("%s:shared", name))
                        .channelFactory(channelFactory).statsLogger(statsLogger);
            } else {
                this.bookKeeperClientBuilder = bkcBuilder;
            }
            bookKeeperClient = this.bookKeeperClientBuilder.build();

            closed = false;
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new DLInterruptedException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
        }

        // Stats
        if (null == handlerStatsLogger) {
            handlerStatsLogger = statsLogger.scope("handlers");
            createWriteHandlerStats = handlerStatsLogger.getOpStatsLogger("create_write_handler");
        }
    }

    @Override
    public synchronized void registerListener(LogSegmentListener listener) throws IOException {
        if (null == readHandlerForListener) {
            readHandlerForListener = createReadLedgerHandler(DistributedLogConstants.DEFAULT_STREAM);
            readHandlerForListener.registerListener(listener);
            readHandlerForListener.scheduleGetLedgersTask(true, true);
        } else {
            readHandlerForListener.registerListener(listener);
        }
    }

    @Override
    public synchronized void unregisterListener(LogSegmentListener listener) {
        if (null != readHandlerForListener) {
            readHandlerForListener.unregisterListener(listener);
        }
    }

    public void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (closed) {
            throw new AlreadyClosedException("Executing " + operation + " on already closed DistributedLogManager");
        }

        if (null != bookKeeperClient) {
            bookKeeperClient.checkClosedOrInError();
        }
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(PartitionId partition) throws IOException {
        return createReadLedgerHandler(partition.toString(), null);
    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(PartitionId partition) throws IOException {
        return createWriteLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier) throws IOException {
        return createReadLedgerHandler(streamIdentifier, null);
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier,
                                                                          AsyncNotification notification) throws IOException {
        return new BKLogPartitionReadHandler(name, streamIdentifier, conf, uri,
                zooKeeperClientBuilder, bookKeeperClientBuilder, executorService, statsLogger, notification);
    }

    public BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier) throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            BKLogPartitionWriteHandler handler = doCreateWriteLedgerHandler(streamIdentifier);
            success = true;
            return handler;
        } finally {
            if (success) {
                createWriteHandlerStats.registerSuccessfulEvent(stopwatch.stop().elapsedMillis());
            } else {
                createWriteHandlerStats.registerFailedEvent(stopwatch.stop().elapsedMillis());
            }
        }
    }

    synchronized public BKLogPartitionWriteHandler doCreateWriteLedgerHandler(String streamIdentifier) throws IOException {
        BKLogPartitionWriteHandler writeHandler =
            BKLogPartitionWriteHandler.createBKLogPartitionWriteHandler(name, streamIdentifier, conf, uri,
                zooKeeperClientBuilder, bookKeeperClientBuilder, executorService, ledgerAllocator, statsLogger, clientId, regionId);
        PermitManager manager = getLogSegmentRollingPermitManager();
        if (manager instanceof Watcher) {
            writeHandler.register((Watcher) manager);
        }
        return writeHandler;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    int getRegionId() {
        return regionId;
    }

    void setRegionId(int regionId) {
        this.regionId = regionId;
    }

    public synchronized void setLedgerAllocator(LedgerAllocator allocator) {
        this.ledgerAllocator = allocator;
    }

    PermitManager getLogSegmentRollingPermitManager() {
        return logSegmentRollingPermitManager;
    }

    void setLogSegmentRollingPermitManager(PermitManager manager) {
        this.logSegmentRollingPermitManager = manager;
    }

    /**
     * Check if an end of stream marker was added to the stream for the partition
     * A stream with an end of stream marker cannot be appended to
     *
     * @return true if the marker was added to the stream, false otherwise
     */
    @Override
    public boolean isEndOfStreamMarked(PartitionId partition) throws IOException {
        throw new NotYetImplementedException("isEndOfStreamMarked for partitioned streams");
    }

    /**
     * Check if an end of stream marker was added to the stream
     * A stream with an end of stream marker cannot be appended to
     *
     * @return true if the marker was added to the stream, false otherwise
     */
    @Override
    public boolean isEndOfStreamMarked() throws IOException {
        return (getLastTxIdInternal(DistributedLogConstants.DEFAULT_STREAM, false, true) == DistributedLogConstants.MAX_TXID);
    }

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException {
        long position;
        try {
            position = getLastTxIdInternal(DistributedLogConstants.DEFAULT_STREAM, true, false);
            if (DistributedLogConstants.INVALID_TXID == position ||
                DistributedLogConstants.EMPTY_LEDGER_TX_ID == position) {
                position = 0;
            }
        } catch (LogEmptyException lee) {
            // Start with position zero
            //
            position = 0;
        }
        return new AppendOnlyStreamWriter(startLogSegmentNonPartitioned(), position);
    }

    /**
     * Get a reader to read a log stream as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamReader getAppendOnlyStreamReader() throws IOException {
        return new AppendOnlyStreamReader(this);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public PartitionAwareLogWriter startLogSegment() throws IOException {
        checkClosedOrInError("startLogSegment");
        return new BKPartitionAwareLogWriter(conf, this);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public synchronized BKUnPartitionedSyncLogWriter startLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        return new BKUnPartitionedSyncLogWriter(conf, this);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public synchronized AsyncLogWriter startAsyncLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        initializeFuturePool(true);
        // proactively recover incomplete logsegments for async log writer
        return new BKUnPartitionedAsyncLogWriter(conf, this, orderedFuturePool).recover();
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param partition – the partition (stream) within the log to read from
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public LogReader getInputStream(PartitionId partition, long fromTxnId)
        throws IOException {
        return getInputStreamInternal(partition.toString(), fromTxnId);
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public LogReader getInputStream(long fromTxnId)
        throws IOException {
        return getInputStreamInternal(DistributedLogConstants.DEFAULT_STREAM, fromTxnId);
    }

    @Override
    public LogReader getInputStream(PartitionId partition, DLSN fromDLSN) throws IOException {
        return getInputStreamInternal(partition.toString(), fromDLSN);
    }

    @Override
    public LogReader getInputStream(DLSN fromDLSN) throws IOException {
        return getInputStreamInternal(DistributedLogConstants.DEFAULT_STREAM, fromDLSN);
    }

    @Override
    public AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException {
        throw new NotYetImplementedException("getAsyncLogReader");
    }

    @Override
    public AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException {
        return new BKAsyncLogReaderDLSN(this, executorService, DistributedLogConstants.DEFAULT_STREAM, fromDLSN);
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param streamIdentifier
     * @param fromTxnId
     * @return
     * @throws IOException
     */
    public LogReader getInputStreamInternal(String streamIdentifier, long fromTxnId)
        throws IOException {
        checkClosedOrInError("getInputStream");
        return new BKContinuousLogReaderTxId(this, streamIdentifier, fromTxnId, conf.getEnableReadAhead(), false, null);
    }

    LogReader getInputStreamInternal(String streamIdentifier, DLSN dlsn) throws IOException {
        checkClosedOrInError("getInputStream");
        return new BKContinuousLogReaderDLSN(this, streamIdentifier, dlsn, conf.getEnableReadAhead(), false, null);
    }

    /**
     * Get the last log record no later than the specified transactionId
     *
     * @param partition – the partition within the log stream to read from
     * @param thresholdTxId - the threshold transaction id
     * @return the last log record before a given transactionId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public long getTxIdNotLaterThan(PartitionId partition, long thresholdTxId) throws IOException {
        return getTxIdNotLaterThanInternal(partition.toString(), thresholdTxId);
    }

    @Override
    public long getTxIdNotLaterThan(long thresholdTxId) throws IOException {
        return getTxIdNotLaterThanInternal(DistributedLogConstants.DEFAULT_STREAM, thresholdTxId);
    }

    private long getTxIdNotLaterThanInternal(String streamIdentifier, long thresholdTxId) throws IOException {
        checkClosedOrInError("getTxIdNotLaterThan");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        long returnValue = ledgerHandler.getTxIdNotLaterThan(thresholdTxId);
        ledgerHandler.close();
        return returnValue;
    }

    /**
     * Get the last log record in the stream
     *
     * @param partition – the partition within the log stream to read from
     * @return the last log record in the stream
     * @throws java.io.IOException if a stream cannot be found.
     */
    @Override
    public LogRecordWithDLSN getLastLogRecord(PartitionId partition) throws IOException {
        return getLastLogRecordInternal(partition.toString());
    }

    /**
     * Get the last log record in the stream
     *
     * @return the last log record in the stream
     * @throws java.io.IOException if a stream cannot be found.
     */
    @Override
    public LogRecordWithDLSN getLastLogRecord() throws IOException {
        return getLastLogRecordInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    private LogRecordWithDLSN getLastLogRecordInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getLastLogRecord");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLastLogRecord(false, false);
        } finally {
            ledgerHandler.close();
        }
    }

    @Override
    public long getFirstTxId(PartitionId partition) throws IOException {
        return getFirstTxIdInternal(partition.toString());
    }

    @Override
    public long getFirstTxId() throws IOException {
        return getFirstTxIdInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    private long getFirstTxIdInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getFirstTxId");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getFirstTxId();
        } finally {
            ledgerHandler.close();
        }
    }

    @Override
    public long getLastTxId(PartitionId partition) throws IOException {
        return getLastTxIdInternal(partition.toString(), false, false);
    }

    @Override
    public long getLastTxId() throws IOException {
        return getLastTxIdInternal(DistributedLogConstants.DEFAULT_STREAM, false, false);
    }

    private long getLastTxIdInternal(String streamIdentifier, boolean recover, boolean includeEndOfStream) throws IOException {
        checkClosedOrInError("getLastTxId");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLastTxId(recover, includeEndOfStream);
        } finally {
            ledgerHandler.close();
        }
    }

    @Override
    public DLSN getLastDLSN(PartitionId partition) throws IOException {
        return getLastDLSNInternal(partition.toString(), false, false);
    }

    @Override
    public DLSN getLastDLSN() throws IOException {
        return getLastDLSNInternal(DistributedLogConstants.DEFAULT_STREAM, false, false);
    }

    private DLSN getLastDLSNInternal(String streamIdentifier, boolean recover, boolean includeEndOfStream) throws IOException {
        checkClosedOrInError("getLastDLSN");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLastDLSN(recover, includeEndOfStream);
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Get Latest Transaction Id in the specified partition of the log
     *
     * @param partition - the partition within the log
     * @return latest transaction id
     */
    @Override
    public Future<Long> getLastTxIdAsync(PartitionId partition) {
        return getLastTxIdAsyncInternal(partition.toString());
    }

    /**
     * Get Latest Transaction Id in the non partitioned stream
     *
     * @return latest transaction id
     */
    @Override
    public Future<Long> getLastTxIdAsync() {
        return getLastTxIdAsyncInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    private Future<Long> getLastTxIdAsyncInternal(final String streamIdentifier) {
        initializeFuturePool(false);
        return readerFuturePool.apply(new ExceptionalFunction0<Long>() {
            public Long applyE() throws IOException {
                return getLastTxIdInternal(streamIdentifier, false, false);
            }
        });
    }


    /**
     * Get Latest DLSN in the specified partition of the log
     *
     * @param partition - the partition within the log
     * @return latest transaction id
     */
    @Override
    public Future<DLSN> getLastDLSNAsync(PartitionId partition) {
        return getLastDLSNAsyncInternal(partition.toString());
    }

    /**
     * Get Latest DLSN in the non partitioned stream
     *
     * @return latest transaction id
     */
    @Override
    public Future<DLSN> getLastDLSNAsync() {
        return getLastDLSNAsyncInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    private Future<DLSN> getLastDLSNAsyncInternal(final String streamIdentifier) {
        initializeFuturePool(false);
        return readerFuturePool.apply(new ExceptionalFunction0<DLSN>() {
            public DLSN applyE() throws IOException {
                return getLastDLSNInternal(streamIdentifier, false, false);
            }
        });
    }

    /**
     * Get the number of log records in the active portion of the stream for the
     * given partition
     * Any log segments that have already been truncated will not be included
     *
     * @param partition the partition within the log
     * @return number of log records
     * @throws IOException
     */
    @Override
    public long getLogRecordCount(PartitionId partition) throws IOException {
        return getLogRecordCountInternal(partition.toString());
    }

    /**
     * Get the number of log records in the active portion of the non-partitioned
     * stream
     * Any log segments that have already been truncated will not be included
     *
     * @return number of log records
     * @throws IOException
     */
    @Override
    public long getLogRecordCount() throws IOException {
        return getLogRecordCountInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    private long getLogRecordCountInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getLogRecordCount");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLogRecordCount();
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Recover a specified partition within the log container
     *
     * @param partition – the partition within the log stream to delete
     * @throws IOException if the recovery fails
     */
    @Override
    public void recover(PartitionId partition) throws IOException {
        recoverInternal(partition.toString());
    }

    /**
     * Recover the default stream within the log container (for
     * un partitioned log containers)
     *
     * @throws IOException if the recovery fails
     */
    @Override
    public void recover() throws IOException {
        recoverInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    /**
     * Recover a specified stream within the log container
     * The writer implicitly recovers a topic when it resumes writing.
     * This allows applications to recover a container explicitly so
     * that application may read a fully recovered partition before resuming
     * the writes
     *
     * @throws IOException if the recovery fails
     */
    private void recoverInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("recoverInternal");
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        try {
            ledgerHandler.recoverIncompleteLogSegments();
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Delete the specified partition
     *
     * @param partition – the partition within the log stream to delete
     * @throws IOException if the deletion fails
     */
    @Override
    public void deletePartition(PartitionId partition) throws IOException {
        deletePartition(partition.toString());
    }

    /**
     * Delete the specified partition
     *
     * @throws IOException if the deletion fails
     */
    public void deletePartition(String streamIdentifier) throws IOException {
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        try {
            ledgerHandler.deleteLog();
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Delete all the partitions of the specified log
     *
     * @throws IOException if the deletion fails
     */
    @Override
    public void delete() throws IOException {
        for (String streamIdentifier : getStreamsWithinALog()) {
            deletePartition(streamIdentifier);
        }

        // Delete the ZK path associated with the log stream
        String zkPath = getZKPath();
        // Safety check when we are using the shared zookeeper
        if (zkPath.toLowerCase().contains("distributedlog")) {
            ZooKeeperClient zkc = zooKeeperClientBuilder.buildNew();
            try {
                LOG.info("Delete the path associated with the log {}, ZK Path", name, zkPath);
                ZKUtil.deleteRecursive(zkc.get(), zkPath);
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while accessing ZK", ie);
                throw new DLInterruptedException("Error initializing zk", ie);
            } catch (KeeperException ke) {
                LOG.error("Error accessing entry in zookeeper", ke);
                throw new IOException("Error initializing zk", ke);
            } finally {
                zkc.close();
            }

        } else {
            LOG.warn("Skip deletion of unrecognized ZK Path {}", zkPath);
        }
    }


    /**
     * The DistributedLogManager may archive/purge any logs for transactionId
     * less than or equal to minImageTxId.
     * This is to be used only when the client explicitly manages deletion. If
     * the cleanup policy is based on sliding time window, then this method need
     * not be called.
     *
     * @param minTxIdToKeep the earliest txid that must be retained
     * @throws IOException if purging fails
     */
    @Override
    public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
        for (String streamIdentifier : getStreamsWithinALog()) {
            purgeLogsForPartitionOlderThan(streamIdentifier, minTxIdToKeep);
        }
    }

    private List<String> getStreamsWithinALog() throws IOException {
        List<String> partitions;
        String zkPath = getZKPath();
        ZooKeeperClient zkc = zooKeeperClientBuilder.buildNew();
        try {
            if (zkc.get().exists(zkPath, false) == null) {
                LOG.info("Log {} was not found, ZK Path {} doesn't exist", name, zkPath);
                throw new LogNotFoundException("Log " + name + " was not found");
            }
            partitions = zkc.get().getChildren(zkPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
        } finally {
            zkc.close();
        }
        return partitions;
    }


    /**
     * The DistributedLogManager may archive/purge any logs for transactionId
     * less than or equal to minImageTxId.
     * This is to be used only when the client explicitly manages deletion. If
     * the cleanup policy is based on sliding time window, then this method need
     * not be called.
     *
     * @param minTxIdToKeep the earliest txid that must be retained
     * @throws IOException if purging fails
     */
    public void purgeLogsForPartitionOlderThan(String streamIdentifier, long minTxIdToKeep) throws IOException {
        checkClosedOrInError("purgeLogsOlderThan");
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        try {
            LOG.info("Purging logs for {} older than {}", ledgerHandler.getFullyQualifiedName(), minTxIdToKeep);
            ledgerHandler.purgeLogsOlderThan(minTxIdToKeep);
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Close the distributed log manager, freeing any resources it may hold.
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (null != readHandlerForListener) {
                readHandlerForListener.close();
            }
        }
        if (ownExecutor) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted when shutting down scheduler : ", e);
            }
            executorService.shutdownNow();
            LOG.info("Stopped BKDL executor service.");
        }
        try {
            bookKeeperClient.release();
            super.close();
        } catch (Exception e) {
            LOG.warn("Exception while closing distributed log manager", e);
        }
        closed = true;
    }

    public boolean scheduleTask(Runnable task) {
        try {
            executorService.submit(task);
            return true;
        } catch (RejectedExecutionException ree) {
            LOG.error("Task {} is rejected : ", ree);
            return false;
        }
    }

    public Watcher registerExpirationHandler(final ZooKeeperClient.ZooKeeperSessionExpireNotifier onExpired) {
        if (conf.getZKNumRetries() > 0) {
            return new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // nop
                }
            };
        }
        return zooKeeperClient.registerExpirationHandler(onExpired);
    }

    public boolean unregister(Watcher watcher) {
        return zooKeeperClient.unregister(watcher);
    }

    private void initializeFuturePool(boolean ordered) {
        // Note for orderedFuturePool:
        // Single Threaded Future Pool inherently preserves order by tasks one by one
        //
        if (ownExecutor) {
            // ownExecutor is a single threaded thread pool
            if (null == orderedFuturePool) {
                // Readers share the same future pool as the orderedFuturePool
                orderedFuturePool = new ExecutorServiceFuturePool(executorService);
                readerFuturePool = orderedFuturePool;
            }
        } else if (ordered && (null == orderedFuturePool)) {
            // When we are using a thread pool that was passed from the factory, we can use
            // the executor service
            orderedFuturePool = new ExecutorServiceFuturePool(Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("BKALW-" + name + "-executor-%d").build()));
        } else if (!ordered && (null == readerFuturePool)) {
            // readerFuturePool can just use the executor service that was configured with the DLM
            readerFuturePool = new ExecutorServiceFuturePool(executorService);
        }
    }
}
