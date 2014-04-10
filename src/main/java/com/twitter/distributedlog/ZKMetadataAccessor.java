package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;

import com.twitter.distributedlog.exceptions.DLInterruptedException;

import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZKMetadataAccessor implements MetadataAccessor {
    static final Logger LOG = LoggerFactory.getLogger(ZKMetadataAccessor.class);
    protected final String name;
    protected boolean zkcClosed = true;
    protected final URI uri;
    protected final ZooKeeperClientBuilder zooKeeperClientBuilder;
    protected final ZooKeeperClient zooKeeperClient;

    public ZKMetadataAccessor(String name,
                              DistributedLogConfiguration conf,
                              URI uri,
                              ZooKeeperClientBuilder zkcBuilder) {
        this.name = name;
        this.uri = uri;

        if (null == zkcBuilder) {
            RetryPolicy retryPolicy = null;
            if (conf.getZKNumRetries() > 0) {
                retryPolicy = new BoundExponentialBackoffRetryPolicy(
                    conf.getZKRetryBackoffStartMillis(),
                    conf.getZKRetryBackoffMaxMillis(), conf.getZKNumRetries());
            }
            this.zooKeeperClientBuilder = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .uri(uri).retryPolicy(retryPolicy).buildNew(false);
        } else {
            this.zooKeeperClientBuilder = zkcBuilder;
        }

        zooKeeperClient = this.zooKeeperClientBuilder.build();
        zkcClosed = false;
    }

    /**
     * Creates or update the metadata stored at the node associated with the
     * name and URI
     * @param metadata opaque metadata to be stored for the node
     * @throws IOException
     */
    @Override
    public void createOrUpdateMetadata(byte[] metadata) throws IOException {
        checkClosedOrInError("createOrUpdateMetadata");

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
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on creating or updating container metadata", ie);
        } catch (Exception exc) {
            throw new IOException("Exception creating or updating container metadata", exc);
        }
    }

    /**
     * Delete the metadata stored at the associated node. This only deletes the metadata
     * and not the node itself
     * @throws IOException
     */
    @Override
    public void deleteMetadata() throws IOException {
        checkClosedOrInError("createOrUpdateMetadata");
        createOrUpdateMetadata(null);
    }

    /**
     * Retrieve the metadata stored at the node
     * @return byte array containing the metadata
     * @throws IOException
     */
    @Override
    public byte[] getMetadata() throws IOException {
        checkClosedOrInError("createOrUpdateMetadata");
        String zkPath = getZKPath();
        LOG.debug("Getting application specific metadata from {}", zkPath);
        try {
            Stat currentStat = zooKeeperClient.get().exists(zkPath, false);
            if (currentStat == null) {
                return null;
            } else {
                return zooKeeperClient.get().getData(zkPath, false, currentStat);
            }
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Error reading the max tx id from zk", ie);
        } catch (Exception e) {
            throw new IOException("Error reading the max tx id from zk", e);
        }
    }

    /**
     * Close the metadata accessor, freeing any resources it may hold.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        try {
            zooKeeperClient.close();
        } catch (Exception e) {
            LOG.warn("Exception while closing distributed log manager", e);
        }
        zkcClosed = true;
    }

    public void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (zkcClosed) {
            throw new AlreadyClosedException("Executing " + operation + " on already closed ZKMetadataAccessor");
        }
    }

    protected String getZKPath() {
        return String.format("%s/%s", uri.getPath(), name);
    }
}
