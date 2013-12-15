package com.twitter.distributedlog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.util.ExecutorServiceFuturePool;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
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

    private String clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
    private final DistributedLogConfiguration conf;
    private boolean closed = true;
    private final ScheduledExecutorService executorService;
    private boolean ownExecutor;
    private final BookKeeperClientBuilder bookKeeperClientBuilder;
    private final BookKeeperClient bookKeeperClient;
    private final StatsLogger statsLogger;
    private ExecutorServiceFuturePool futurePool = null;

    public BKDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                   ZooKeeperClientBuilder zkcBuilder, BookKeeperClientBuilder bkcBuilder,
                                   StatsLogger statsLogger) throws IOException {
        this(name, conf, uri, zkcBuilder, bkcBuilder,
                Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("BKDL-" + name + "-executor-%d").build()),
                statsLogger);
        this.ownExecutor = true;
    }

    public BKDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri,
                                   ZooKeeperClientBuilder zkcBuilder,
                                   BookKeeperClientBuilder bkcBuilder,
                                   ScheduledExecutorService executorService,
                                   StatsLogger statsLogger) throws IOException {
        super(name, uri, conf.getZKSessionTimeoutMilliseconds(), zkcBuilder);
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
                this.bookKeeperClientBuilder = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(conf).bkdlConfig(bkdlConfig).name(String.format("%s:shared", name))
                        .statsLogger(statsLogger);
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

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier) throws IOException {
        return new BKLogPartitionWriteHandler(name, streamIdentifier, conf, uri,
                zooKeeperClientBuilder, bookKeeperClientBuilder, executorService, statsLogger, clientId);
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
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
        if (null == futurePool) {
            if (ownExecutor) {
                futurePool = new ExecutorServiceFuturePool(executorService);
            } else {
                futurePool = new ExecutorServiceFuturePool(Executors.newScheduledThreadPool(1,
                    new ThreadFactoryBuilder().setNameFormat("BKALW-" + name + "-executor-%d").build()));
            }
        }

        return new BKUnPartitionedAsyncLogWriter(conf, this, futurePool);
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
    public AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException {
        throw new NotYetImplementedException("getAsyncLogReader");
    }

    @Override
    public AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException {
        return new BKAsyncLogReaderDLSN(this, executorService, DistributedLogConstants.DEFAULT_STREAM, fromDLSN, conf.getReadAheadWaitTime());
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
        return new BKContinuousLogReaderTxId(this, streamIdentifier, fromTxnId, conf.getEnableReadAhead(), conf.getReadAheadWaitTime(), false, null);
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
        return zooKeeperClient.registerExpirationHandler(onExpired);
    }

    public boolean unregister(Watcher watcher) {
        return zooKeeperClient.unregister(watcher);
    }

}
