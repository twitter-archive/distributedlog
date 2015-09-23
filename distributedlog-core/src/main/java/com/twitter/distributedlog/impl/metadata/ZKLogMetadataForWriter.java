package com.twitter.distributedlog.impl.metadata;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.zk.DataWithStat;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Log Metadata for writer
 */
public class ZKLogMetadataForWriter extends ZKLogMetadata {

    static final Logger LOG = LoggerFactory.getLogger(ZKLogMetadataForWriter.class);

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

    private static class IgnoreNodeExistsStringCallback implements org.apache.zookeeper.AsyncCallback.StringCallback {
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
    private static final IgnoreNodeExistsStringCallback IGNORE_NODE_EXISTS_STRING_CALLBACK = new IgnoreNodeExistsStringCallback();

    private static class MultiGetCallback implements org.apache.zookeeper.AsyncCallback.DataCallback {

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

    public static Future<ZKLogMetadataForWriter> createLogIfNotExists(
            final URI uri,
            final String logName,
            final String logIdentifier,
            final ZooKeeper zk,
            final List<ACL> acl,
            final boolean ownAllocator) {
        final String logRootPath = ZKLogMetadata.getLogRootPath(uri, logName, logIdentifier);
        try {
            PathUtils.validatePath(logRootPath);
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal path value {} for stream {}", new Object[]{logRootPath, logName, e});
            return Future.exception(new InvalidStreamNameException(logName, "Log name is invalid"));
        }

        // Note re. persistent lock state initialization: the read lock persistent state (path) is
        // initialized here but only used in the read handler. The reason is its more convenient and
        // less error prone to manage all stream structure in one place.
        final String logSegmentsPath = logRootPath + LOGSEGMENTS_PATH;
        final String maxTxIdPath = logRootPath + MAX_TXID_PATH;
        final String lockPath = logRootPath + LOCK_PATH;
        final String readLockPath = logRootPath + READ_LOCK_PATH;
        final String versionPath = logRootPath + VERSION_PATH;
        final String allocationPath = logRootPath + ALLOCATION_PATH;

        final DataWithStat allocationData = new DataWithStat();
        final DataWithStat maxTxIdData = new DataWithStat();
        final DataWithStat logSegmentsData = new DataWithStat();
        // lockData, readLockData & logSegmentsData just used for checking whether the path exists or not.
        final DataWithStat lockData = new DataWithStat();
        final DataWithStat readLockData = new DataWithStat();
        final DataWithStat versionData = new DataWithStat();

        final Promise<ZKLogMetadataForWriter> promise = new Promise<ZKLogMetadataForWriter>();

        final Runnable completion = new Runnable() {
            @Override
            public void run() {
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
                    promise.setException(new UnexpectedException("Invalid log " + logRootPath, iae));
                    return;
                }

                ZKLogMetadataForWriter logMetadata =
                        new ZKLogMetadataForWriter(uri, logName, logIdentifier, logSegmentsData, maxTxIdData, allocationData);
                promise.setValue(logMetadata);
            }
        };

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
                    LOG.error("Failed to create log root path {} : ", logRootPath, ke);
                    promise.setException(
                            new ZKException("Failed to create log root path " + logRootPath, ke));
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
                // log segments path
                zk.create(logSegmentsPath, new byte[]{'0'}, acl,
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
                    promise.setException(new ZKException("Failed to get data from path " + path, ke));
                    return;
                }
                if (numPendings.decrementAndGet() == 0) {
                    completion.run();
                }
            }
        }

        class GetDataCallback implements org.apache.zookeeper.AsyncCallback.VoidCallback {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    if ((ownAllocator && allocationData.notExists()) || versionData.notExists() || maxTxIdData.notExists() ||
                        lockData.notExists() || readLockData.notExists() || logSegmentsData.notExists()) {
                        ZkUtils.asyncCreateFullPathOptimistic(zk, logRootPath, new byte[]{'0'},
                                acl, CreateMode.PERSISTENT, new CreatePartitionCallback(), null);
                    } else {
                        completion.run();
                    }
                } else {
                    LOG.error("Failed to get log data from {}.", logRootPath);
                    promise.setException(new ZKException("Failed to get partiton data from " + logRootPath,
                            KeeperException.Code.get(rc)));
                    return;
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
        zk.getData(logSegmentsPath, false, getCallback, logSegmentsData);

        return promise;
    }

    public static Future<ZKLogMetadataForWriter> getLogMetadata(final URI uri,
                                                                final String logName,
                                                                final String logIdentifier,
                                                                final ZooKeeper zk) {
        final String logSegmentsPath = getLogRootPath(uri, logName, logIdentifier);
        final Promise<ZKLogMetadataForWriter> promise =
                new Promise<ZKLogMetadataForWriter>();

        zk.getData(logSegmentsPath, false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    DataWithStat logSegmentsStat = new DataWithStat();
                    DataWithStat maxTXIdStat = new DataWithStat();
                    DataWithStat allocationStat = new DataWithStat();
                    logSegmentsStat.setDataWithStat(data, stat);
                    ZKLogMetadataForWriter logMetadata =
                        new ZKLogMetadataForWriter(uri, logName, logIdentifier,
                                logSegmentsStat, maxTXIdStat, allocationStat);
                    promise.setValue(logMetadata);
                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                    promise.setException(new LogNotFoundException("No " + LOGSEGMENTS_PATH
                            + " found for log " + logName + ":" + logIdentifier));
                } else {
                    promise.setException(new ZKException("Failed on getting logsegments data from " + logSegmentsPath + " : ",
                            KeeperException.Code.get(rc)));
                }
            }
        }, null);
        return promise;
    }

    private final DataWithStat maxTxIdStat;
    private final DataWithStat logSegmentsStat;
    private final DataWithStat allocationStat;

    /**
     * metadata representation of a log
     *
     * @param uri           namespace to store the log
     * @param logName       name of the log
     * @param logIdentifier identifier of the log
     */
    private ZKLogMetadataForWriter(URI uri,
                                   String logName,
                                   String logIdentifier,
                                   DataWithStat logSegmentsStat,
                                   DataWithStat maxTxIdStat,
                                   DataWithStat allocationStat) {
        super(uri, logName, logIdentifier);
        this.logSegmentsStat = logSegmentsStat;
        this.maxTxIdStat = maxTxIdStat;
        this.allocationStat = allocationStat;
    }

    public DataWithStat getLogSegmentsStat() {
        return logSegmentsStat;
    }

    public DataWithStat getMaxTxIdStat() {
        return maxTxIdStat;
    }

    public DataWithStat getAllocationStat() {
        return allocationStat;
    }

}
