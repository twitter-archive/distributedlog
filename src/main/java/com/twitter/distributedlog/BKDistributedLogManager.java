package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.data.Stat;


class BKDistributedLogManager implements DistributedLogManager {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogManager.class);

    private final String name;
    private final DistributedLogConfiguration conf;
    private final URI uri;
    private boolean closed = true;
    private ExecutorService cbThreadPool = null;
    private BookKeeperClient bookKeeperClient = null;
    private ZooKeeperClient zooKeeperClient = null;
    private boolean separateZKClient = false;
    private Timer periodicTimer = null;

    public BKDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri) throws IOException {
        this(name, conf, uri, null, null);
    }

    public BKDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri, ZooKeeperClient zkc, BookKeeperClient bkc) throws IOException {
        this.name = name;
        this.conf = conf;
        this.uri = uri;

        try {
            // Distributed Log Manager always creates a zookeeper connection to
            // handle session expiration
            // Bookkeeper client is only created if separate BK clients option is
            // not specified
            if (null == zkc) {
                zooKeeperClient = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
                separateZKClient = true;
            } else {
                zooKeeperClient = zkc;
            }

            if (null == bkc) {
                if (conf.getShareZKClientWithBKC()) {
                    this.bookKeeperClient = new BookKeeperClient(conf, zooKeeperClient, String.format("%s:shared", name));
                } else {
                    this.bookKeeperClient = new BookKeeperClient(conf, uri.getAuthority().replace(";", ","), String.format("%s:shared", name));
                }
            } else {
                bookKeeperClient = bkc;
                bookKeeperClient.addRef();
            }

            if (conf.getPeriodicFlushFrequencyMilliSeconds() > 0) {
                periodicTimer = new Timer();
            }

            closed = false;
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
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
        return createReadLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(PartitionId partition) throws IOException {
        return createWriteLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier) throws IOException {
        return new BKLogPartitionReadHandler(name, streamIdentifier, conf, uri, zooKeeperClient, bookKeeperClient);

    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier) throws IOException {
        return new BKLogPartitionWriteHandler(name, streamIdentifier, conf, uri, zooKeeperClient, bookKeeperClient, periodicTimer);
    }

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException {
        long position = 0;
        try {
            position = getLastTxIdInternal(DistributedLogConstants.DEFAULT_STREAM, true);
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
    public synchronized BKUnPartitionedLogWriter startLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        return new BKUnPartitionedLogWriter(conf, this);
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
     * @param partition – the partition within the log stream to read from
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public LogReader getInputStream(long fromTxnId)
        throws IOException {
        return getInputStreamInternal(DistributedLogConstants.DEFAULT_STREAM, fromTxnId);
    }

    public LogReader getInputStreamInternal(String streamIdentifier, long fromTxnId)
        throws IOException {
        checkClosedOrInError("getInputStream");
        return new BKContinuousLogReader(this, streamIdentifier, fromTxnId, conf.getEnableReadAhead(), conf.getReadAheadWaitTime());
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


    @Override
    public long getFirstTxId(PartitionId partition) throws IOException {
        return getFirstTxIdInternal(partition.toString());
    }

    @Override
    public long getFirstTxId() throws IOException {
        return getFirstTxIdInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    private long getFirstTxIdInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getFirstTxIdInternal");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        long returnValue = ledgerHandler.getFirstTxId();
        ledgerHandler.close();
        return returnValue;
    }

    @Override
    public long getLastTxId(PartitionId partition) throws IOException {
        return getLastTxIdInternal(partition.toString(), false);
    }

    @Override
    public long getLastTxId() throws IOException {
        return getLastTxIdInternal(DistributedLogConstants.DEFAULT_STREAM, false);
    }

    private long getLastTxIdInternal(String streamIdentifier, boolean recover) throws IOException {
        checkClosedOrInError("getLastTxIdInternal");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        long returnValue = ledgerHandler.getLastTxId(recover);
        ledgerHandler.close();
        return returnValue;
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
     * @param partition – the partition within the log stream to delete
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
     * @param partition – the partition within the log stream to delete
     * @throws IOException if the recovery fails
     */
    private void recoverInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("recoverInternal");
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        ledgerHandler.recoverIncompleteLogSegments();
        ledgerHandler.close();
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
     * @param partition – the partition within the log stream to delete
     * @throws IOException if the deletion fails
     */
    public void deletePartition(String streamIdentifier) throws IOException {
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        ledgerHandler.deleteLog();
        ledgerHandler.close();
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
            ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
            try {
                LOG.info("Delete the path associated with the log {}, ZK Path", name, zkPath);
                ZKUtil.deleteRecursive(zkc.get(), zkPath);
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while accessing ZK", ie);
                throw new IOException("Error initializing zk", ie);
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
        ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
        try {
            if (zkc.get().exists(zkPath, false) == null) {
                LOG.info("Log {} was not found, ZK Path {} doesn't exist", name, zkPath);
                throw new LogNotFoundException("Log" + name + "was not found");
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
        LOG.info("Purging logs for {} older than {}", ledgerHandler.getFullyQualifiedName(), minTxIdToKeep);
        ledgerHandler.purgeLogsOlderThan(minTxIdToKeep);
        ledgerHandler.close();
    }

    /**
     * Close the distributed log manager, freeing any resources it may hold.
     */
    @Override
    public void close() throws IOException {
        try {
            if (null != bookKeeperClient) {
                bookKeeperClient.release();
                bookKeeperClient = null;
            }
            if (separateZKClient && (null != zooKeeperClient)) {
                zooKeeperClient.close();
                zooKeeperClient = null;
            }
            if (null != cbThreadPool) {
                cbThreadPool.shutdown();
                cbThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
                cbThreadPool.shutdownNow();
            }
        } catch (Exception e) {
            LOG.warn("Exception while closing distributed log manager", e);
        }
        closed = true;
    }

    public Future<?> enqueueBackgroundTask(Runnable task) {
        if (null == cbThreadPool) {
            cbThreadPool = Executors.newFixedThreadPool(1, new DaemonThreadFactory());
        }
        return cbThreadPool.submit(task);
    }

    public Watcher registerExpirationHandler(final ZooKeeperClient.ZooKeeperSessionExpireNotifier onExpired) {
        return zooKeeperClient.registerExpirationHandler(onExpired);
    }

    public boolean unregister(Watcher watcher) {
        return zooKeeperClient.unregister(watcher);
    }

    public void createOrUpdateMetadata(byte[] metadata) throws IOException {
        String zkPath = getZKPath();
        LOG.debug("Setting application specific metadata on {}", zkPath);
        try {
            Stat currentStat = zooKeeperClient.get().exists(zkPath, false);
            if (currentStat == null) {
                if (metadata.length > 0) {
                    Utils.zkCreateFullPathOptimistic(zooKeeperClient,
                        zkPath,
                        metadata,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                }
            } else {
                zooKeeperClient.get().setData(zkPath, metadata, currentStat.getVersion());
            }
        } catch (Exception exc) {
            throw new IOException("Exception creating or updating container metadata", exc);
        }
    }

    public void deleteMetadata() throws IOException {
        createOrUpdateMetadata(new byte[0]);
    }

    public byte[] getMetadata() throws IOException {
        String zkPath = getZKPath();
        LOG.debug("Getting application specific metadata from {}", zkPath);
        try {
            Stat currentStat = zooKeeperClient.get().exists(zkPath, false);
            if (currentStat == null) {
                return null;
            } else {
                return zooKeeperClient.get().getData(zkPath, false, currentStat);
            }
        } catch (Exception e) {
            throw new IOException("Error reading the max tx id from zk", e);
        }
    }

    private String getZKPath() {
        return String.format("%s%s/%s", conf.getDLZKPathPrefix(), uri.getPath(), name);
    }

}
