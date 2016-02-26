package com.twitter.distributedlog.v2;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AppendOnlyStreamReader;
import com.twitter.distributedlog.AppendOnlyStreamWriter;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.LogWriter;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.subscription.SubscriptionStateStore;
import com.twitter.distributedlog.subscription.SubscriptionsStore;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.util.Future;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

class BKDistributedLogManager extends ZKMetadataAccessor implements DistributedLogManager {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogManager.class);

    static String getPartitionPath(URI uri, String streamName, String streamIdentifier) {
        return String.format("%s/%s/%s", uri.getPath(), streamName, streamIdentifier);
    }

    static void createUnpartitionedStream(DistributedLogConfiguration conf, ZooKeeperClient zkc, URI uri, String streamName)
            throws IOException {
        BKLogPartitionWriteHandler.createStreamIfNotExists(getPartitionPath(uri, streamName,
                conf.getUnpartitionedStreamName()), zkc);
    }

    private String clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
    private final DistributedLogConfiguration conf;
    private boolean closed = true;
    private final OrderedScheduler scheduler;
    private boolean ownExecutor;
    private OrderedScheduler lockStateExecutor;
    // bookkeeper clients
    // NOTE: The actual bookkeeper client is initialized lazily when it is referenced by
    //       {@link com.twitter.distributedlog.v2.BookKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    private final BookKeeperClientBuilder writerBKCBuilder;
    private final BookKeeperClient writerBKC;
    private final boolean ownWriterBKC;
    private final BookKeeperClientBuilder readerBKCBuilder;
    private final BookKeeperClient readerBKC;
    private final boolean ownReaderBKC;
    private final StatsLogger statsLogger;

    private final PermitLimiter writeLimiter;

    BKDistributedLogManager(String name,
                            DistributedLogConfiguration conf,
                            URI uri,
                            ZooKeeperClientBuilder writerZKCBuilder,
                            ZooKeeperClientBuilder readerZKCBuilder,
                            ZooKeeperClient zkcForWriterBKC,
                            ZooKeeperClient zkcForReaderBKC,
                            BookKeeperClientBuilder writerBKCBuilder,
                            BookKeeperClientBuilder readerBKCBuilder,
                            PermitLimiter writeLimiter,
                            StatsLogger statsLogger) throws IOException {
        this(name, conf, uri,
             writerZKCBuilder, readerZKCBuilder,
             zkcForWriterBKC, zkcForReaderBKC, writerBKCBuilder, readerBKCBuilder,
             OrderedScheduler.newBuilder().name("BKDL-" + name).corePoolSize(1).build(),
             null, null, null, writeLimiter, statsLogger);
        this.ownExecutor = true;
    }

    BKDistributedLogManager(String name,
                            DistributedLogConfiguration conf,
                            URI uri,
                            ZooKeeperClientBuilder writerZKCBuilder,
                            ZooKeeperClientBuilder readerZKCBuilder,
                            ZooKeeperClient zkcForWriterBKC,
                            ZooKeeperClient zkcForReaderBKC,
                            BookKeeperClientBuilder writerBKCBuilder,
                            BookKeeperClientBuilder readerBKCBuilder,
                            OrderedScheduler scheduler,
                            OrderedScheduler lockStateExecutor,
                            ClientSocketChannelFactory channelFactory,
                            HashedWheelTimer requestTimer,
                            PermitLimiter writeLimiter,
                            StatsLogger statsLogger) throws IOException {
        super(name, conf, uri, writerZKCBuilder, readerZKCBuilder, statsLogger);
        this.conf = conf;
        this.scheduler = scheduler;
        this.lockStateExecutor = lockStateExecutor;
        this.statsLogger = statsLogger;
        this.ownExecutor = false;
        this.writeLimiter = writeLimiter;

        // create the bkc for writers
        if (null == writerBKCBuilder) {
            // resolve uri
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(writerZKC, uri);
            this.writerBKCBuilder = BookKeeperClientBuilder.newBuilder()
                    .dlConfig(conf)
                    .name(String.format("bk:%s:dlm_writer_shared", name))
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .channelFactory(channelFactory)
                    .requestTimer(requestTimer)
                    .statsLogger(statsLogger);
            if (null == zkcForWriterBKC) {
                this.writerBKCBuilder.zkServers(bkdlConfig.getBkZkServersForWriter());
            } else {
                this.writerBKCBuilder.zkc(zkcForWriterBKC);
            }
            this.ownWriterBKC = true;
        } else {
            this.writerBKCBuilder = writerBKCBuilder;
            this.ownWriterBKC = false;
        }
        this.writerBKC = this.writerBKCBuilder.build();

        // create the bkc for readers
        if (null == readerBKCBuilder) {
            // resolve uri
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(writerZKC, uri);
            if (bkdlConfig.getBkZkServersForWriter().equals(bkdlConfig.getBkZkServersForReader())) {
                this.readerBKCBuilder = this.writerBKCBuilder;
                this.ownReaderBKC = false;
            } else {
                this.readerBKCBuilder = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(conf)
                        .name(String.format("bk:%s:dlm_reader_shared", name))
                        .ledgersPath(bkdlConfig.getBkLedgersPath())
                        .channelFactory(channelFactory)
                        .requestTimer(requestTimer)
                        .statsLogger(statsLogger);
                if (null == zkcForReaderBKC) {
                    this.readerBKCBuilder.zkServers(bkdlConfig.getBkZkServersForReader());
                } else {
                    this.readerBKCBuilder.zkc(zkcForReaderBKC);
                }
                this.ownReaderBKC = true;
            }
        } else {
            this.readerBKCBuilder = readerBKCBuilder;
            this.ownReaderBKC = false;
        }
        this.readerBKC = this.readerBKCBuilder.build();

        closed = false;
    }

    private synchronized OrderedScheduler getLockStateExecutor(boolean createIfNull) {
        if (createIfNull && null == lockStateExecutor && ownExecutor) {
            lockStateExecutor = OrderedScheduler.newBuilder()
                    .corePoolSize(1).name("BKDL-LockState").build();
        }
        return lockStateExecutor;
    }

    @VisibleForTesting
    BookKeeperClient getWriterBKC() {
        return this.writerBKC;
    }

    @VisibleForTesting
    BookKeeperClient getReaderBKC() {
        return this.readerBKC;
    }

    public void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (closed) {
            throw new AlreadyClosedException("Executing " + operation + " on already closed DistributedLogManager");
        }

        if (null != writerBKC) {
            writerBKC.checkClosedOrInError();
        }
        if (null != readerBKC) {
            readerBKC.checkClosedOrInError();
        }
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(PartitionId partition) throws IOException {
        return createReadLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(PartitionId partition) throws IOException {
        return createWriteLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier) throws IOException {
        return new BKLogPartitionReadHandler(name, streamIdentifier, conf, uri,
                readerZKCBuilder, readerBKCBuilder, scheduler, statsLogger);
    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier) throws IOException {
        return new BKLogPartitionWriteHandler(name, streamIdentifier, conf, uri,
                writerZKCBuilder, writerBKCBuilder, scheduler, getLockStateExecutor(true),
                statsLogger, clientId, writeLimiter);
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Check if an end of stream marker was added to the stream
     * A stream with an end of stream marker cannot be appended to
     *
     * @return true if the marker was added to the stream, false otherwise
     */
    @Override
    public boolean isEndOfStreamMarked() throws IOException {
        return (getLastTxIdInternal(conf.getUnpartitionedStreamName(), false, true) == DistributedLogConstants.MAX_TXID);
    }

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException {
        throw new NotYetImplementedException("Please use v3 version for append only stream writer");
    }

    /**
     * Get a reader to read a log stream as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamReader getAppendOnlyStreamReader() throws IOException {
        throw new NotYetImplementedException("Please use v3 version for append only stream reader");
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
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
    public synchronized LogWriter startLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        return new BKUnPartitionedSyncLogWriter(conf, this);
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param partition – the partition (stream) within the log to read from
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
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
        return getInputStreamInternal(conf.getUnpartitionedStreamName(), fromTxnId);
    }

    public LogReader getInputStreamInternal(String streamIdentifier, long fromTxnId)
        throws IOException {
        checkClosedOrInError("getInputStream");
        return new BKContinuousLogReader(this, streamIdentifier, fromTxnId, conf, statsLogger);
    }


    /**
     * Get the last log record no later than the specified transactionId
     *
     * @param partition – the partition within the log stream to read from
     * @param thresholdTxId - the threshold transaction id
     * @return the last log record before a given transactionId
     * @throws IOException if a stream cannot be found.
     */
    public long getTxIdNotLaterThan(PartitionId partition, long thresholdTxId) throws IOException {
        return getTxIdNotLaterThanInternal(partition.toString(), thresholdTxId);
    }

    public long getTxIdNotLaterThan(long thresholdTxId) throws IOException {
        return getTxIdNotLaterThanInternal(conf.getUnpartitionedStreamName(), thresholdTxId);
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
     * @return the last log record in the stream
     * @throws java.io.IOException if a stream cannot be found.
     */
    @Override
    public LogRecordWithDLSN getLastLogRecord() throws IOException {
        return getLastLogRecordInternal(conf.getUnpartitionedStreamName());
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

    public long getFirstTxId(PartitionId partition) throws IOException {
        return getFirstTxIdInternal(partition.toString());
    }

    @Override
    public long getFirstTxId() throws IOException {
        return getFirstTxIdInternal(conf.getUnpartitionedStreamName());
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
    public long getLastTxId() throws IOException {
        return getLastTxIdInternal(conf.getUnpartitionedStreamName(), false, false);
    }

    public long getLastTxId(PartitionId partition) throws IOException {
        return getLastTxIdInternal(partition.toString(), false, false);
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

    /**
     * Get the number of log records in the active portion of the stream for the
     * given partition
     * Any log segments that have already been truncated will not be included
     *
     * @param partition the partition within the log
     * @return number of log records
     * @throws IOException
     */
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
        return getLogRecordCountInternal(conf.getUnpartitionedStreamName());
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
        recoverInternal(conf.getUnpartitionedStreamName());
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
            try {
                LOG.info("Delete the path associated with the log {}, ZK Path", name, zkPath);
                ZKUtil.deleteRecursive(writerZKC.get(), zkPath);
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while accessing ZK", ie);
                throw new DLInterruptedException("Error initializing zk", ie);
            } catch (KeeperException ke) {
                LOG.error("Error accessing entry in zookeeper", ke);
                throw new IOException("Error initializing zk", ke);
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
        try {
            if (readerZKC.get().exists(zkPath, false) == null) {
                LOG.info("Log {} was not found, ZK Path {} doesn't exist", name, zkPath);
                throw new LogNotFoundException("Log " + name + " was not found");
            }
            partitions = readerZKC.get().getChildren(zkPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new DLInterruptedException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
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
        if (ownExecutor) {
            SchedulerUtils.shutdownScheduler(scheduler, 5000, TimeUnit.MILLISECONDS);
            LOG.info("Stopped BKDL executor service for {}.", name);
            SchedulerUtils.shutdownScheduler(getLockStateExecutor(false), 5000, TimeUnit.MILLISECONDS);
            LOG.info("Stopped BKDL Lock State Executor for {}.", name);
        }
        if (ownWriterBKC) {
            writerBKC.close();
        }
        if (ownReaderBKC) {
            readerBKC.close();
        }
        try {
            super.close();
        } catch (IOException e) {
            LOG.warn("Exception while closing distributed log manager {} : ", name, e);
        }
        closed = true;
    }

    public boolean scheduleTask(Runnable task) {
        try {
            scheduler.submit(task);
            return true;
        } catch (RejectedExecutionException ree) {
            LOG.error("Task {} is rejected : ", ree);
            return false;
        }
    }

    // v3 methods


    @Override
    public List<LogSegmentMetadata> getLogSegments() throws IOException {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public void registerListener(LogSegmentListener listener) throws IOException {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public void unregisterListener(LogSegmentListener listener) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public AsyncLogWriter startAsyncLogSegmentNonPartitioned() throws IOException {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public LogReader getInputStream(DLSN fromDLSN) throws IOException {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<AsyncLogReader> openAsyncLogReader(long fromTxnId) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<AsyncLogReader> openAsyncLogReader(DLSN fromDLSN) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN, String subscriberId) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(String subscriberId) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<DLSN> getDLSNNotLessThanTxId(long transactionId) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public DLSN getLastDLSN() throws IOException {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<LogRecordWithDLSN> getLastLogRecordAsync() {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<Long> getLastTxIdAsync() {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<DLSN> getFirstDLSNAsync() {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<DLSN> getLastDLSNAsync() {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public Future<Long> getLogRecordCountAsync(DLSN beginDLSN) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public SubscriptionStateStore getSubscriptionStateStore(String subscriberId) {
        throw new UnsupportedOperationException("Not supported in v2");
    }

    @Override
    public SubscriptionsStore getSubscriptionsStore() {
        throw new UnsupportedOperationException("Not supported in v2");
    }
}
