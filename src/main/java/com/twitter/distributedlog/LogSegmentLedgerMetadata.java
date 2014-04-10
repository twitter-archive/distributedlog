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

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing the metadata associated
 * with a single edit log segment, stored in a single ledger
 */
class LogSegmentLedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LogSegmentLedgerMetadata.class);

    private String zkPath;
    private final long ledgerId;
    private final int version;
    private final long firstTxId;
    private long lastTxId;
    private long completionTime;
    private int recordCount;
    private boolean inprogress;
    // zk version
    private Integer zkVersion = null;

    public static final Comparator COMPARATOR
        = new Comparator<LogSegmentLedgerMetadata>() {

        public int compare(LogSegmentLedgerMetadata o1,
                           LogSegmentLedgerMetadata o2) {
            if (o1.firstTxId < o2.firstTxId) {
                return -1;
            } else if (o1.firstTxId == o2.firstTxId) {
                return 0;
            } else {
                return 1;
            }
        }
    };

    public static final Comparator DESC_COMPARATOR
        = new Comparator<LogSegmentLedgerMetadata>() {
        public int compare(LogSegmentLedgerMetadata o1,
                           LogSegmentLedgerMetadata o2) {
            if (o1.firstTxId > o2.firstTxId) {
                return -1;
            } else if (o1.firstTxId == o2.firstTxId) {
                return 0;
            } else {
                return 1;
            }
        }
    };

    static final int LOGRECORD_COUNT_SHIFT = 32;
    static final long LOGRECORD_COUNT_MASK = 0xffffffff00000000L;
    static final long METADATA_VERSION_MASK = 0x00000000ffffffffL;

    LogSegmentLedgerMetadata(String zkPath, int version,
                             long ledgerId, long firstTxId) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = DistributedLogConstants.INVALID_TXID;
        this.inprogress = true;
    }

    private LogSegmentLedgerMetadata(String zkPath, int version, long ledgerId,
                             long firstTxId, long lastTxId, long completionTime, int recordCount) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = lastTxId;
        this.inprogress = false;
        this.completionTime = completionTime;
        this.recordCount = recordCount;
    }

    String getZkPath() {
        return zkPath;
    }

    int getZkVersion() {
        return null == zkVersion ? -1 : zkVersion;
    }

    long getFirstTxId() {
        return firstTxId;
    }

    long getLastTxId() {
        return lastTxId;
    }

    long getCompletionTime() {
        return completionTime;
    }

    long getLedgerId() {
        return ledgerId;
    }

    long getLedgerSequenceNumber() {
        return firstTxId;
    }

    int getVersion() {
        return version;
    }

    public int getRecordCount() {
        return recordCount;
    }

    boolean isInProgress() {
        return this.inprogress;
    }

    long finalizeLedger(long newLastTxId, int recordCount) {
        assert this.lastTxId == DistributedLogConstants.INVALID_TXID;
        this.lastTxId = newLastTxId;
        this.inprogress = false;
        this.completionTime = Utils.nowInMillis();
        this.recordCount = recordCount;
        return this.completionTime;
    }

    static LogSegmentLedgerMetadata read(ZooKeeperClient zkc, String path)
        throws IOException, KeeperException.NoNodeException {
        try {
            Stat stat = new Stat();
            byte[] data = zkc.get().getData(path, false, stat);
            LogSegmentLedgerMetadata metadata = parseData(path, data);
            metadata.zkVersion = stat.getVersion();
            return metadata;
        } catch (KeeperException.NoNodeException nne) {
            throw nne;
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on reading log segment metadata from " + path, ie);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zce) {
            throw new IOException("Encountered zookeeper connection issue when reading log segment metadata from "
                    + path, zce);
        } catch (KeeperException ke) {
            throw new IOException("Encountered zookeeper issue when reading log segment metadata from " + path, ke);
        }
    }

    static void read(ZooKeeperClient zkc, String path,
                     final BookkeeperInternalCallbacks.GenericCallback<LogSegmentLedgerMetadata> callback) {
        try {
            zkc.get().getData(path, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        callback.operationComplete(BKException.Code.ZKException, null);
                        return;
                    }
                    try {
                        LogSegmentLedgerMetadata metadata = parseData(path, data);
                        metadata.zkVersion = stat.getVersion();
                        callback.operationComplete(BKException.Code.OK, metadata);
                    } catch (IOException ie) {
                        // as we don't have return code for distributedlog. for now, we leveraged bk
                        // exception code.
                        callback.operationComplete(BKException.Code.IncorrectParameterException, null);
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            callback.operationComplete(BKException.Code.ZKException, null);
        } catch (InterruptedException e) {
            callback.operationComplete(BKException.Code.InterruptedException, null);
        }
    }

    static LogSegmentLedgerMetadata parseData(String path, byte[] data) throws IOException {
        String[] parts = new String(data, UTF_8).split(";");
        if (parts.length == 3) {
            int version = Integer.valueOf(parts[0]);
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            return new LogSegmentLedgerMetadata(path, version, ledgerId, txId);
        } else if (parts.length == 5) {
            long versionAndCount = Long.valueOf(parts[0]);

            long version = versionAndCount & METADATA_VERSION_MASK;
            assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);

            long recordCount = (versionAndCount & LOGRECORD_COUNT_MASK) >> LOGRECORD_COUNT_SHIFT;
            assert (recordCount >= Integer.MIN_VALUE && recordCount <= Integer.MAX_VALUE);

            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            return new LogSegmentLedgerMetadata(path, (int)version, ledgerId,
                firstTxId, lastTxId, completionTime, (int)recordCount);
        } else {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }
    }

    void write(ZooKeeperClient zkc, String path)
        throws IOException, KeeperException.NodeExistsException {
        this.zkPath = path;
        String finalisedData;
        if (inprogress) {
            finalisedData = String.format("%d;%d;%d",
                version, ledgerId, firstTxId);
        } else {
            long versionAndCount = ((long) version) | ((long)recordCount << LOGRECORD_COUNT_SHIFT);
            finalisedData = String.format("%d;%d;%d;%d;%d",
                versionAndCount, ledgerId, firstTxId, lastTxId, completionTime);
        }
        try {
            zkc.get().create(path, finalisedData.getBytes(UTF_8),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zkVersion = 0;
        } catch (KeeperException.NodeExistsException nee) {
            throw nee;
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on creating ledger znode " + path, ie);
        } catch (Exception e) {
            LOG.error("Error creating ledger znode {}", path, e);
            throw new IOException("Error creating ledger znode" + path);
        }
    }

    boolean checkEquivalence(ZooKeeperClient zkc, String path) {
        try {
            LogSegmentLedgerMetadata other = read(zkc, path);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Verifying {} against {}", this, other);
            }

            boolean retVal;

            // All fields may not be comparable so only compare the ones
            // that can be compared
            // completionTime is set when a node is finalized, so that
            // cannot be compared
            // if the node is inprogress, don't compare the lastTxId either
            if (this.ledgerId != other.ledgerId ||
                this.firstTxId != other.firstTxId ||
                this.version != other.version) {
                retVal = false;
            } else if (this.inprogress) {
                retVal = other.inprogress;
            } else {
                retVal = (!other.inprogress && (this.lastTxId == other.lastTxId));
            }

            if (!retVal) {
                LOG.warn("Equivalence check failed between {} and {}", this, other);
            }

            return retVal;
        } catch (Exception e) {
            LOG.error("Could not check equivalence between:" + this + " and data in " + path, e);
            return false;
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof LogSegmentLedgerMetadata)) {
            return false;
        }
        LogSegmentLedgerMetadata ol = (LogSegmentLedgerMetadata) o;
        return ledgerId == ol.ledgerId
            && firstTxId == ol.firstTxId
            && lastTxId == ol.lastTxId
            && version == ol.version
            && completionTime == ol.completionTime;
    }

    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + (int) ledgerId;
        hash = hash * 31 + (int) firstTxId;
        hash = hash * 31 + (int) lastTxId;
        hash = hash * 31 + version;
        hash = hash * 31 + (int) completionTime;
        return hash;
    }

    public String toString() {
        return "[LedgerId:" + ledgerId +
            ", firstTxId:" + firstTxId +
            ", lastTxId:" + lastTxId +
            ", version:" + version +
            ", completionTime:" + completionTime +
            ", recordCount" + recordCount + "]";
    }
}
