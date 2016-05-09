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
package com.twitter.distributedlog.impl.metadata;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.exceptions.LogExistsException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

import java.io.File;
import java.net.URI;
import java.util.List;

/**
 * Log Metadata for writer
 */
public class ZKLogMetadataForWriter extends ZKLogMetadata {

    static final Logger LOG = LoggerFactory.getLogger(ZKLogMetadataForWriter.class);

    static class MetadataIndex {
        static final int LOG_ROOT_PARENT = 0;
        static final int LOG_ROOT = 1;
        static final int MAX_TXID = 2;
        static final int VERSION = 3;
        static final int LOCK = 4;
        static final int READ_LOCK = 5;
        static final int LOGSEGMENTS = 6;
        static final int ALLOCATION = 7;
    }

    static int bytesToInt(byte[] b) {
        assert b.length >= 4;
        return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
    }

    static byte[] intToBytes(int i) {
        return new byte[]{
            (byte) (i >> 24),
            (byte) (i >> 16),
            (byte) (i >> 8),
            (byte) (i)};
    }

    static boolean pathExists(Versioned<byte[]> metadata) {
        return null != metadata.getValue() && null != metadata.getVersion();
    }

    static void ensureMetadataExist(Versioned<byte[]> metadata) {
        Preconditions.checkNotNull(metadata.getValue());
        Preconditions.checkNotNull(metadata.getVersion());
    }

    public static Future<ZKLogMetadataForWriter> of(
            final URI uri,
            final String logName,
            final String logIdentifier,
            final ZooKeeper zk,
            final List<ACL> acl,
            final boolean ownAllocator,
            final boolean createIfNotExists) {
        final String logRootPath = ZKLogMetadata.getLogRootPath(uri, logName, logIdentifier);
        try {
            PathUtils.validatePath(logRootPath);
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal path value {} for stream {}", new Object[]{logRootPath, logName, e});
            return Future.exception(new InvalidStreamNameException(logName, "Log name is invalid"));
        }

        return checkLogMetadataPaths(zk, logRootPath, ownAllocator)
                .flatMap(new AbstractFunction1<List<Versioned<byte[]>>, Future<List<Versioned<byte[]>>>>() {
                    @Override
                    public Future<List<Versioned<byte[]>>> apply(List<Versioned<byte[]>> metadatas) {
                        Promise<List<Versioned<byte[]>>> promise =
                                new Promise<List<Versioned<byte[]>>>();
                        createMissingMetadata(zk, logRootPath, metadatas, acl,
                                ownAllocator, createIfNotExists, promise);
                        return promise;
                    }
                }).map(new ExceptionalFunction<List<Versioned<byte[]>>, ZKLogMetadataForWriter>() {
                    @Override
                    public ZKLogMetadataForWriter applyE(List<Versioned<byte[]>> metadatas) throws DLException {
                        return processLogMetadatas(uri, logName, logIdentifier, metadatas, ownAllocator);
                    }
                });
    }

    static Future<List<Versioned<byte[]>>> checkLogMetadataPaths(ZooKeeper zk,
                                                                 String logRootPath,
                                                                 boolean ownAllocator) {
        // Note re. persistent lock state initialization: the read lock persistent state (path) is
        // initialized here but only used in the read handler. The reason is its more convenient and
        // less error prone to manage all stream structure in one place.
        final String logRootParentPath = new File(logRootPath).getParent();
        final String logSegmentsPath = logRootPath + LOGSEGMENTS_PATH;
        final String maxTxIdPath = logRootPath + MAX_TXID_PATH;
        final String lockPath = logRootPath + LOCK_PATH;
        final String readLockPath = logRootPath + READ_LOCK_PATH;
        final String versionPath = logRootPath + VERSION_PATH;
        final String allocationPath = logRootPath + ALLOCATION_PATH;

        int numPaths = ownAllocator ? MetadataIndex.ALLOCATION + 1 : MetadataIndex.LOGSEGMENTS + 1;
        List<Future<Versioned<byte[]>>> checkFutures = Lists.newArrayListWithExpectedSize(numPaths);
        checkFutures.add(Utils.zkGetData(zk, logRootParentPath, false));
        checkFutures.add(Utils.zkGetData(zk, logRootPath, false));
        checkFutures.add(Utils.zkGetData(zk, maxTxIdPath, false));
        checkFutures.add(Utils.zkGetData(zk, versionPath, false));
        checkFutures.add(Utils.zkGetData(zk, lockPath, false));
        checkFutures.add(Utils.zkGetData(zk, readLockPath, false));
        checkFutures.add(Utils.zkGetData(zk, logSegmentsPath, false));
        if (ownAllocator) {
            checkFutures.add(Utils.zkGetData(zk, allocationPath, false));
        }

        return Future.collect(checkFutures);
    }

    static void createMissingMetadata(final ZooKeeper zk,
                                      final String logRootPath,
                                      final List<Versioned<byte[]>> metadatas,
                                      final List<ACL> acl,
                                      final boolean ownAllocator,
                                      final boolean createIfNotExists,
                                      final Promise<List<Versioned<byte[]>>> promise) {
        final List<byte[]> pathsToCreate = Lists.newArrayListWithExpectedSize(metadatas.size());
        final List<Op> zkOps = Lists.newArrayListWithExpectedSize(metadatas.size());
        CreateMode createMode = CreateMode.PERSISTENT;

        // log root parent path
        if (pathExists(metadatas.get(MetadataIndex.LOG_ROOT_PARENT))) {
            pathsToCreate.add(null);
        } else {
            String logRootParentPath = new File(logRootPath).getParent();
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootParentPath, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }

        // log root path
        if (pathExists(metadatas.get(MetadataIndex.LOG_ROOT))) {
            pathsToCreate.add(null);
        } else {
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootPath, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }

        // max id
        if (pathExists(metadatas.get(MetadataIndex.MAX_TXID))) {
            pathsToCreate.add(null);
        } else {
            byte[] zeroTxnIdData = DLUtils.serializeTransactionId(0L);
            pathsToCreate.add(zeroTxnIdData);
            zkOps.add(Op.create(logRootPath + MAX_TXID_PATH, zeroTxnIdData, acl, createMode));
        }
        // version
        if (pathExists(metadatas.get(MetadataIndex.VERSION))) {
            pathsToCreate.add(null);
        } else {
            byte[] versionData = intToBytes(LAYOUT_VERSION);
            pathsToCreate.add(versionData);
            zkOps.add(Op.create(logRootPath + VERSION_PATH, versionData, acl, createMode));
        }
        // lock path
        if (pathExists(metadatas.get(MetadataIndex.LOCK))) {
            pathsToCreate.add(null);
        } else {
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootPath + LOCK_PATH, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }
        // read lock path
        if (pathExists(metadatas.get(MetadataIndex.READ_LOCK))) {
            pathsToCreate.add(null);
        } else {
            pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
            zkOps.add(Op.create(logRootPath + READ_LOCK_PATH, DistributedLogConstants.EMPTY_BYTES, acl, createMode));
        }
        // log segments path
        if (pathExists(metadatas.get(MetadataIndex.LOGSEGMENTS))) {
            pathsToCreate.add(null);
        } else {
            byte[] logSegmentsData = DLUtils.serializeLogSegmentSequenceNumber(
                    DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO);
            pathsToCreate.add(logSegmentsData);
            zkOps.add(Op.create(logRootPath + LOGSEGMENTS_PATH, logSegmentsData, acl, createMode));
        }
        // allocation path
        if (ownAllocator) {
            if (pathExists(metadatas.get(MetadataIndex.ALLOCATION))) {
                pathsToCreate.add(null);
            } else {
                pathsToCreate.add(DistributedLogConstants.EMPTY_BYTES);
                zkOps.add(Op.create(logRootPath + ALLOCATION_PATH,
                        DistributedLogConstants.EMPTY_BYTES, acl, createMode));
            }
        }
        if (zkOps.isEmpty()) {
            // nothing missed
            promise.setValue(metadatas);
            return;
        }
        if (!createIfNotExists) {
            promise.setException(new LogNotFoundException("Log " + logRootPath + " not found"));
            return;
        }

        zk.multi(zkOps, new AsyncCallback.MultiCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<OpResult> resultList) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    List<Versioned<byte[]>> finalMetadatas =
                            Lists.newArrayListWithExpectedSize(metadatas.size());
                    for (int i = 0; i < pathsToCreate.size(); i++) {
                        byte[] dataCreated = pathsToCreate.get(i);
                        if (null == dataCreated) {
                            finalMetadatas.add(metadatas.get(i));
                        } else {
                            finalMetadatas.add(new Versioned<byte[]>(dataCreated, new ZkVersion(0)));
                        }
                    }
                    promise.setValue(finalMetadatas);
                } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                    promise.setException(new LogExistsException("Someone just created log "
                            + logRootPath));
                } else {
                    if (LOG.isDebugEnabled()) {
                        StringBuilder builder = new StringBuilder();
                        for (OpResult result : resultList) {
                            if (result instanceof OpResult.ErrorResult) {
                                OpResult.ErrorResult errorResult = (OpResult.ErrorResult) result;
                                builder.append(errorResult.getErr()).append(",");
                            } else {
                                builder.append(0).append(",");
                            }
                        }
                        String resultCodeList = builder.substring(0, builder.length() - 1);
                        LOG.debug("Failed to create log, full rc list = {}", resultCodeList);
                    }

                    promise.setException(new ZKException("Failed to create log " + logRootPath,
                            KeeperException.Code.get(rc)));
                }
            }
        }, null);
    }

    static ZKLogMetadataForWriter processLogMetadatas(URI uri,
                                                      String logName,
                                                      String logIdentifier,
                                                      List<Versioned<byte[]>> metadatas,
                                                      boolean ownAllocator)
            throws UnexpectedException {
        try {
            // max id
            Versioned<byte[]> maxTxnIdData = metadatas.get(MetadataIndex.MAX_TXID);
            ensureMetadataExist(maxTxnIdData);
            // version
            Versioned<byte[]> versionData = metadatas.get(MetadataIndex.VERSION);
            ensureMetadataExist(maxTxnIdData);
            Preconditions.checkArgument(LAYOUT_VERSION == bytesToInt(versionData.getValue()));
            // lock path
            ensureMetadataExist(metadatas.get(MetadataIndex.LOCK));
            // read lock path
            ensureMetadataExist(metadatas.get(MetadataIndex.READ_LOCK));
            // max lssn
            Versioned<byte[]> maxLSSNData = metadatas.get(MetadataIndex.LOGSEGMENTS);
            ensureMetadataExist(maxLSSNData);
            try {
                DLUtils.deserializeLogSegmentSequenceNumber(maxLSSNData.getValue());
            } catch (NumberFormatException nfe) {
                throw new UnexpectedException("Invalid max sequence number found in log " + logName, nfe);
            }
            // allocation path
            Versioned<byte[]>  allocationData;
            if (ownAllocator) {
                allocationData = metadatas.get(MetadataIndex.ALLOCATION);
                ensureMetadataExist(allocationData);
            } else {
                allocationData = new Versioned<byte[]>(null, null);
            }
            return new ZKLogMetadataForWriter(uri, logName, logIdentifier,
                    maxLSSNData, maxTxnIdData, allocationData);
        } catch (IllegalArgumentException iae) {
            throw new UnexpectedException("Invalid log " + logName, iae);
        } catch (NullPointerException npe) {
            throw new UnexpectedException("Invalid log " + logName, npe);
        }
    }

    private final Versioned<byte[]> maxLSSNData;
    private final Versioned<byte[]> maxTxIdData;
    private final Versioned<byte[]> allocationData;

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
                                   Versioned<byte[]> maxLSSNData,
                                   Versioned<byte[]> maxTxIdData,
                                   Versioned<byte[]> allocationData) {
        super(uri, logName, logIdentifier);
        this.maxLSSNData = maxLSSNData;
        this.maxTxIdData = maxTxIdData;
        this.allocationData = allocationData;
    }

    public Versioned<byte[]> getMaxLSSNData() {
        return maxLSSNData;
    }

    public Versioned<byte[]> getMaxTxIdData() {
        return maxTxIdData;
    }

    public Versioned<byte[]> getAllocationData() {
        return allocationData;
    }

}
