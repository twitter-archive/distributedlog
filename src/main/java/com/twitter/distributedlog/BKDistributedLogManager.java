package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;


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
            String zkConnect = uri.getAuthority().replace(";", ",");
            int zkSessionTimeout = conf.getZKSessionTimeoutSeconds() * 1000;
            zkcShared = new ZooKeeperClient(zkSessionTimeout, 2 * zkSessionTimeout, zkConnect);

            if (!conf.getSeparateBKClients()) {
                if (conf.getShareZKClientWithBKC()) {
                    this.bkcShared = new BookKeeperClient(conf, zkcShared, String.format("%s:shared", name));
                } else {
                    this.bkcShared = new BookKeeperClient(conf, zkConnect, String.format("%s:shared", name));
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
        return new BKLogPartitionReadHandler(name, partition, conf, uri, conf.getSeparateZKClients() ? null : zkcShared, bkcShared);

    }

    public BKLogPartitionWriteHandler createWriteLedgerHandler(PartitionId partition) throws IOException {
        return new BKLogPartitionWriteHandler(name, partition, conf, uri, conf.getSeparateZKClients() ? null : zkcShared, bkcShared);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @param name  - the name of the log stream to write to
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
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param partition – the partition within the log stream to read from
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public LogReader getInputStream(PartitionId partition, long fromTxnId)
        throws IOException {
        checkClosedOrInError("getInputStream");
        return new BKContinuousLogReader(this, partition, fromTxnId, conf.getEnableReadAhead(), conf.getReadAheadWaitTime());
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
    public long getTxIdNotLaterThan(PartitionId partition, long thresholdTxId)
        throws IOException {
        checkClosedOrInError("getTxIdNotLaterThan");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(partition);
        long returnValue = ledgerHandler.getTxIdNotLaterThan(thresholdTxId);
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
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(partition);
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
        String zkConnect = uri.getAuthority().replace(";", ",");
        String zkPath = conf.getDLZKPathPrefix() + uri.getPath() + String.format("/%s", name);
        try {
            ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds() * 1000,
                conf.getZKSessionTimeoutSeconds() * 1000, zkConnect);

            if (zkc.get().exists(zkPath, false) == null) {
                throw new LogNotFoundException("Log" + name + "was not found");
            }
            List<String> partitions = zkc.get().getChildren(zkPath, false);

            for (String partition : partitions) {
                LOG.info("Name:" + name + " Partition:" + partition);
                deletePartition(new PartitionId(Integer.parseInt(partition)));
            }
            LOG.info("Delete the path associated with the log:" + zkPath);

            if (zkPath.toLowerCase().contains("distributedlog")) {
                ZKUtil.deleteRecursive(zkc.get(), zkPath);
            } else {
                LOG.warn("Skip deletion of unrecognized ZK Path {}", zkPath);
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
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
        String zkConnect = uri.getAuthority().replace(";", ",");
        String zkPath = conf.getDLZKPathPrefix() + uri.getPath() + String.format("/%s", name);
        try {
            ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds() * 1000,
                conf.getZKSessionTimeoutSeconds() * 1000, zkConnect);

            if (zkc.get().exists(zkPath, false) == null) {
                throw new LogNotFoundException("Log" + name + "was not found");
            }
            List<String> partitions = zkc.get().getChildren(zkPath, false);

            for (String partition : partitions) {
                LOG.info("Name:" + name + " Partition:" + partition);
                purgeLogsForPartitionOlderThan(new PartitionId(Integer.parseInt(partition)), minTxIdToKeep);
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
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
    public void purgeLogsForPartitionOlderThan(PartitionId partition, long minTxIdToKeep) throws IOException {
        checkClosedOrInError("purgeLogsOlderThan");
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(partition);
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
