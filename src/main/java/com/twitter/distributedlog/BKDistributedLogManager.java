package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class BKDistributedLogManager implements DistributedLogManager {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogManager.class);

    private final String name;
    private final DistributedLogConfiguration conf;
    private final URI uri;
    private boolean closed = true;
    private ExecutorService cbThreadPool = null;
    private BookKeeperClient bkcShared = null;
    private ZooKeeperClient zkcShared = null;

    public BKDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri) throws IOException {
        this.name = name;
        this.conf = conf;
        this.uri = uri;

        try {
            // Distributed Log Manager always creates a zookeeper connection to
            // handle session expiration
            // Bookkeeper client is only created if separate BK clients option is
            // not specified
            zkcShared = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);

            if (!conf.getSeparateBKClients()) {
                if (conf.getShareZKClientWithBKC()) {
                    this.bkcShared = new BookKeeperClient(conf, zkcShared, String.format("%s:shared", name));
                } else {
                    this.bkcShared = new BookKeeperClient(conf, uri.getAuthority().replace(";", ","), String.format("%s:shared", name));
                }
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

        if (null != bkcShared) {
            bkcShared.checkClosedOrInError();
        }
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(PartitionId partition) throws IOException {
        return createReadLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(PartitionId partition) throws IOException {
        return createWriteLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier) throws IOException {
        return new BKLogPartitionReadHandler(name, streamIdentifier, conf, uri, conf.getSeparateZKClients() ? null : zkcShared, bkcShared);

    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier) throws IOException {
        return new BKLogPartitionWriteHandler(name, streamIdentifier, conf, uri, conf.getSeparateZKClients() ? null : zkcShared, bkcShared);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public PartitionAwareLogWriter startLogSegment() throws IOException {
        checkClosedOrInError("startLogSegment");
        if (null == cbThreadPool) {
            cbThreadPool = Executors.newFixedThreadPool(1, new DaemonThreadFactory());
        }
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
        if (null == cbThreadPool) {
            cbThreadPool = Executors.newFixedThreadPool(1, new DaemonThreadFactory());
        }
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
        return getLastTxIdInternal(partition.toString());
    }

    @Override
    public long getLastTxId() throws IOException {
        return getLastTxIdInternal(DistributedLogConstants.DEFAULT_STREAM);
    }

    private long getLastTxIdInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getLastTxIdInternal");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        long returnValue = ledgerHandler.getLastTxId();
        ledgerHandler.close();
        return returnValue;
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
        String zkPath = conf.getDLZKPathPrefix() + uri.getPath() + String.format("/%s", name);
        LOG.info("Delete the path associated with the log:" + zkPath);
        // Safety check when we are using the shared zookeeper
        if (zkPath.toLowerCase().contains("distributedlog")) {
            try {
                ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
                ZKUtil.deleteRecursive(zkc.get(), zkPath);
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while accessing ZK", ie);
                throw new IOException("Error initializing zk", ie);
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
        String zkPath = conf.getDLZKPathPrefix() + uri.getPath() + String.format("/%s", name);
        try {
            ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);

            if (zkc.get().exists(zkPath, false) == null) {
                throw new LogNotFoundException("Log" + name + "was not found");
            }
            partitions = zkc.get().getChildren(zkPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
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
            if (null != bkcShared) {
                bkcShared.release();
            }
            if (null != zkcShared) {
                zkcShared.close();
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
        return cbThreadPool.submit(task);
    }

    public Watcher registerExpirationHandler(final ZooKeeperClient.ZooKeeperSessionExpireNotifier onExpired) {
        return zkcShared.registerExpirationHandler(onExpired);
    }

    public boolean unregister(Watcher watcher) {
        return zkcShared.unregister(watcher);
    }
}
