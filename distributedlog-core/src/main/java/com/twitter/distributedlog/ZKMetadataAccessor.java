/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.exceptions.AlreadyClosedException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKMetadataAccessor implements MetadataAccessor {
    static final Logger LOG = LoggerFactory.getLogger(ZKMetadataAccessor.class);
    protected final String name;
    protected Promise<Void> closePromise;
    protected final URI uri;
    // zookeeper clients
    // NOTE: The actual zookeeper client is initialized lazily when it is referenced by
    //       {@link com.twitter.distributedlog.ZooKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    protected final ZooKeeperClientBuilder writerZKCBuilder;
    protected final ZooKeeperClient writerZKC;
    protected final boolean ownWriterZKC;
    protected final ZooKeeperClientBuilder readerZKCBuilder;
    protected final ZooKeeperClient readerZKC;
    protected final boolean ownReaderZKC;

    ZKMetadataAccessor(String name,
                       DistributedLogConfiguration conf,
                       URI uri,
                       ZooKeeperClientBuilder writerZKCBuilder,
                       ZooKeeperClientBuilder readerZKCBuilder,
                       StatsLogger statsLogger) {
        this.name = name;
        this.uri = uri;

        if (null == writerZKCBuilder) {
            RetryPolicy retryPolicy = null;
            if (conf.getZKNumRetries() > 0) {
                retryPolicy = new BoundExponentialBackoffRetryPolicy(
                    conf.getZKRetryBackoffStartMillis(),
                    conf.getZKRetryBackoffMaxMillis(), conf.getZKNumRetries());
            }
            this.writerZKCBuilder = ZooKeeperClientBuilder.newBuilder()
                    .name(String.format("dlzk:%s:dlm_writer_shared", name))
                    .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                    .retryThreadCount(conf.getZKClientNumberRetryThreads())
                    .requestRateLimit(conf.getZKRequestRateLimit())
                    .zkAclId(conf.getZkAclId())
                    .uri(uri)
                    .retryPolicy(retryPolicy)
                    .statsLogger(statsLogger.scope("dlzk_dlm_writer_shared"));
            this.ownWriterZKC = true;
        } else {
            this.writerZKCBuilder = writerZKCBuilder;
            this.ownWriterZKC = false;
        }
        this.writerZKC = this.writerZKCBuilder.build();

        if (null == readerZKCBuilder) {
            String zkServersForWriter = DLUtils.getZKServersFromDLUri(uri);
            String zkServersForReader;
            try {
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(this.writerZKC, uri);
                zkServersForReader = bkdlConfig.getDlZkServersForReader();
            } catch (IOException e) {
                LOG.warn("Error on resolving dl metadata bindings for {} : ", uri, e);
                zkServersForReader = zkServersForWriter;
            }
            if (zkServersForReader.equals(zkServersForWriter)) {
                LOG.info("Used same zookeeper servers '{}' for both writers and readers for {}.",
                         zkServersForWriter, name);
                this.readerZKCBuilder = this.writerZKCBuilder;
                this.ownReaderZKC = false;
            } else {
                RetryPolicy retryPolicy = null;
                if (conf.getZKNumRetries() > 0) {
                    retryPolicy = new BoundExponentialBackoffRetryPolicy(
                        conf.getZKRetryBackoffStartMillis(),
                        conf.getZKRetryBackoffMaxMillis(), conf.getZKNumRetries());
                }
                this.readerZKCBuilder = ZooKeeperClientBuilder.newBuilder()
                        .name(String.format("dlzk:%s:dlm_reader_shared", name))
                        .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                        .retryThreadCount(conf.getZKClientNumberRetryThreads())
                        .requestRateLimit(conf.getZKRequestRateLimit())
                        .zkServers(zkServersForReader)
                        .retryPolicy(retryPolicy)
                        .zkAclId(conf.getZkAclId())
                        .statsLogger(statsLogger.scope("dlzk_dlm_reader_shared"));
                this.ownReaderZKC = true;
            }
        } else {
            this.readerZKCBuilder = readerZKCBuilder;
            this.ownReaderZKC = false;
        }
        this.readerZKC = this.readerZKCBuilder.build();
    }

    /**
     * Get the name of the stream managed by this log manager
     *
     * @return streamName
     */
    @Override
    public String getStreamName() {
        return name;
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
            Stat currentStat = writerZKC.get().exists(zkPath, false);
            if (currentStat == null) {
                if (metadata.length > 0) {
                    Utils.zkCreateFullPathOptimistic(writerZKC,
                            zkPath,
                            metadata,
                            writerZKC.getDefaultACL(),
                            CreateMode.PERSISTENT);
                }
            } else {
                writerZKC.get().setData(zkPath, metadata, currentStat.getVersion());
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
            Stat currentStat = readerZKC.get().exists(zkPath, false);
            if (currentStat == null) {
                return null;
            } else {
                return readerZKC.get().getData(zkPath, false, currentStat);
            }
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Error reading the max tx id from zk", ie);
        } catch (Exception e) {
            throw new IOException("Error reading the max tx id from zk", e);
        }
    }

    /**
     * Close the metadata accessor, freeing any resources it may hold.
     * @return future represents the close result.
     */
    @Override
    public Future<Void> asyncClose() {
        Promise<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new Promise<Void>();
        }
        // NOTE: ownWriterZKC and ownReaderZKC are mostly used by tests
        //       the managers created by the namespace - whose zkc will be closed by namespace
        try {
            if (ownWriterZKC) {
                writerZKC.close();
            }
            if (ownReaderZKC) {
                readerZKC.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception while closing distributed log manager", e);
        }
        FutureUtils.setValue(closeFuture, null);
        return closeFuture;
    }

    @Override
    public void close() throws IOException {
        FutureUtils.result(asyncClose());
    }

    public synchronized void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (null != closePromise) {
            throw new AlreadyClosedException("Executing " + operation + " on already closed ZKMetadataAccessor");
        }
    }

    protected String getZKPath() {
        return String.format("%s/%s", uri.getPath(), name);
    }

    @VisibleForTesting
    protected ZooKeeperClient getReaderZKC() {
        return readerZKC;
    }

    @VisibleForTesting
    protected ZooKeeperClient getWriterZKC() {
        return writerZKC;
    }
}
