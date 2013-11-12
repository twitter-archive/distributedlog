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
public class LogSegmentLedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LogSegmentLedgerMetadata.class);

    private String zkPath;
    private final long ledgerId;
    private final int version;
    private final long firstTxId;
    private long lastTxId;
    private long completionTime;
    private long ledgerSequenceNumber = DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO;
    private long lastEntryId = -1;
    private long lastSlotId = -1;
    private boolean inprogress;
    // zk version
    private Integer zkVersion = null;

    public static final Comparator COMPARATOR
        = new Comparator<LogSegmentLedgerMetadata>() {

        public int compare(LogSegmentLedgerMetadata o1,
                           LogSegmentLedgerMetadata o2) {
            if ((o1.ledgerSequenceNumber == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO) ||
                (o2.ledgerSequenceNumber == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO)) {
                if (o1.firstTxId < o2.firstTxId) {
                    return -1;
                } else if (o1.firstTxId == o2.firstTxId) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (o1.ledgerSequenceNumber < o2.ledgerSequenceNumber) {
                    return -1;
                } else if (o1.ledgerSequenceNumber == o2.ledgerSequenceNumber) {
                    return 0;
                } else {
                    return 1;
                }
            }


        }
    };

    public static final Comparator DESC_COMPARATOR
        = new Comparator<LogSegmentLedgerMetadata>() {
        public int compare(LogSegmentLedgerMetadata o1,
                           LogSegmentLedgerMetadata o2) {
            if ((o1.ledgerSequenceNumber == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO) ||
                (o2.ledgerSequenceNumber == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO)) {
                if (o1.firstTxId > o2.firstTxId) {
                    return -1;
                } else if (o1.firstTxId == o2.firstTxId) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (o1.ledgerSequenceNumber > o2.ledgerSequenceNumber) {
                    return -1;
                } else if (o1.ledgerSequenceNumber == o2.ledgerSequenceNumber) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }
    };


    public LogSegmentLedgerMetadata(String zkPath,
                                    int version,
                                    long ledgerId,
                                    long firstTxId,
                                    long ledgerSequenceNumber) {

        this(zkPath,
            version,
            ledgerId,
            firstTxId,
            DistributedLogConstants.INVALID_TXID,
            0,
            true,
            ledgerSequenceNumber,
            -1,
            -1);
    }

    public LogSegmentLedgerMetadata(String zkPath,
                                     int version,
                                    long ledgerId,
                                    long firstTxId) {
        this(zkPath,
            version,
            ledgerId,
            firstTxId,
            DistributedLogConstants.INVALID_TXID,
            0,
            true,
            DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO,
            -1,
            -1);
    }

    private LogSegmentLedgerMetadata(String zkPath,
                                     int version,
                                     long ledgerId,
                                     long firstTxId,
                                     long lastTxId,
                                     long completionTime) {
        this(zkPath,
            version,
            ledgerId,
            firstTxId,
            lastTxId,
            completionTime,
            false,
            DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO,
            -1,
            -1);
    }

    private LogSegmentLedgerMetadata(String zkPath,
                                     int version,
                                     long ledgerId,
                                     long firstTxId,
                                     long lastTxId,
                                     long completionTime,
                                     long ledgerSequenceNumber,
                                     long lastEntryId, long lastSlotId) {
        this(zkPath,
            version,
            ledgerId,
            firstTxId,
            lastTxId,
            completionTime,
            false,
            ledgerSequenceNumber,
            lastEntryId,
            lastSlotId);
    }

    private LogSegmentLedgerMetadata(String zkPath,
                                     int version,
                                     long ledgerId,
           long firstTxId, long lastTxId, long completionTime, boolean inprogress,
           long ledgerSequenceNumber, long lastEntryId, long lastSlotId) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = lastTxId;
        this.inprogress = inprogress;
        this.completionTime = completionTime;
        this.ledgerSequenceNumber = ledgerSequenceNumber;
        this.lastEntryId = lastEntryId;
        this.lastSlotId = lastSlotId;
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
        return ledgerSequenceNumber;
    }

    int getVersion() {
        return version;
    }

    public long getLastEntryId() {
        return lastEntryId;
    }

    public long getLastSlotId() {
        return lastSlotId;
    }

    boolean isInProgress() {
        return this.inprogress;
    }

    long finalizeLedger(long newLastTxId, long lastEntryId, long lastSlotId) {
        assert this.lastTxId == DistributedLogConstants.INVALID_TXID;
        this.lastTxId = newLastTxId;
        this.lastEntryId = lastEntryId;
        this.lastSlotId = lastSlotId;
        this.inprogress = false;
        this.completionTime = Utils.nowInMillis();
        return this.completionTime;
    }

    static LogSegmentLedgerMetadata read(ZooKeeperClient zkc, String path, int targetVersion)
        throws IOException, KeeperException.NoNodeException {
        try {
            Stat stat = new Stat();
            byte[] data = zkc.get().getData(path, false, stat);
            LogSegmentLedgerMetadata metadata = parseData(path, data, targetVersion);
            metadata.zkVersion = stat.getVersion();
            return metadata;
        } catch (KeeperException.NoNodeException nne) {
            throw nne;
        } catch (Exception e) {
            throw new IOException("Error reading from zookeeper", e);
        }
    }

    static void read(ZooKeeperClient zkc, String path, final int targetVersion,
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
                        LogSegmentLedgerMetadata metadata = parseData(path, data, targetVersion);
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

    static LogSegmentLedgerMetadata parseDataV1(String path, byte[] data, String[] parts, int targetVersion)
        throws IOException {
        assert (1 == Integer.valueOf(parts[0]));

        if (parts.length == 3) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            return new LogSegmentLedgerMetadata(path, targetVersion, ledgerId, txId);
        } else if (parts.length == 5) {
            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            return new LogSegmentLedgerMetadata(path, targetVersion, ledgerId,
                firstTxId, lastTxId, completionTime);
        } else {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }
    }

    static LogSegmentLedgerMetadata parseDataLatestVersion(String path, byte[] data, String[] parts, int targetVersion)
        throws IOException {
        assert (DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION == Integer.valueOf(parts[0]));
        if (parts.length == 4) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long ledgerSequenceNumber = Long.valueOf(parts[3]);
            return new LogSegmentLedgerMetadata(path, targetVersion, ledgerId, txId, ledgerSequenceNumber);
        } else if (parts.length == 8) {
            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            long ledgerSequenceNumber = Long.valueOf(parts[5]);
            long lastEntryId = Long.valueOf(parts[6]);
            long lastSlotId = Long.valueOf(parts[7]);
            return new LogSegmentLedgerMetadata(path, targetVersion, ledgerId,
                firstTxId, lastTxId, completionTime, ledgerSequenceNumber, lastEntryId, lastSlotId);
        } else {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }

    }

    static LogSegmentLedgerMetadata parseData(String path, byte[] data, int targetVersion) throws IOException {
        String[] parts = new String(data, UTF_8).split(";");
        int version;
        try {
            version = Integer.valueOf(parts[0]);
        } catch (Exception exc) {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }

        if (1 == version) {
            return parseDataV1(path, data, parts, targetVersion);
        } else if (DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION == version) {
            return parseDataLatestVersion(path, data, parts, targetVersion);
        } else {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }
    }

    void write(ZooKeeperClient zkc, String path)
        throws IOException, KeeperException.NodeExistsException {
        this.zkPath = path;
        String finalisedData;
        if (1 == version) {
            if (inprogress) {
                finalisedData = String.format("%d;%d;%d",
                    version, ledgerId, firstTxId);
            } else {
                finalisedData = String.format("%d;%d;%d;%d;%d",
                    version, ledgerId, firstTxId, lastTxId, completionTime);
            }
        } else {
            assert (DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION == version);
            if (inprogress) {
                finalisedData = String.format("%d;%d;%d;%d",
                    version, ledgerId, firstTxId, ledgerSequenceNumber);
            } else {
                finalisedData = String.format("%d;%d;%d;%d;%d;%d;%d;%d",
                    version, ledgerId, firstTxId, lastTxId, completionTime, ledgerSequenceNumber, lastEntryId, lastSlotId);
            }
        }
        try {
            zkc.get().create(path, finalisedData.getBytes(UTF_8),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zkVersion = 0;
        } catch (KeeperException.NodeExistsException nee) {
            throw nee;
        } catch (Exception e) {
            LOG.error("Error creating ledger znode {}", path, e);
            throw new IOException("Error creating ledger znode" + path);
        }
    }

    boolean checkEquivalence(ZooKeeperClient zkc, String path) {
        try {
            LogSegmentLedgerMetadata other = read(zkc, path, version);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Verifying " + this + " against " + other);
            }

            // All fields may not be comparable so only compare the ones
            // that can be compared
            // completionTime is set when a node is finalized, so that
            // cannot be compared
            // if the node is inprogress, don't compare the lastTxId either
            if (this.ledgerSequenceNumber != other.ledgerSequenceNumber ||
                this.ledgerId != other.ledgerId ||
                this.firstTxId != other.firstTxId) {
                return false;
            } else if (this.inprogress) {
                return other.inprogress;
            } else {
                return (!other.inprogress && (this.lastTxId == other.lastTxId));
            }
        } catch (Exception e) {
            LOG.error("Couldn't verify data in " + path, e);
            return false;
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof LogSegmentLedgerMetadata)) {
            return false;
        }
        LogSegmentLedgerMetadata ol = (LogSegmentLedgerMetadata) o;
        return ledgerSequenceNumber == ol.ledgerSequenceNumber
            && ledgerId == ol.ledgerId
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
        hash = hash * 31 + (int) version;
        hash = hash * 31 + (int) completionTime;
        hash = hash * 31 + (int) ledgerSequenceNumber;
        return hash;
    }

    public String toString() {
        return "[LedgerId:" + ledgerId +
            ", firstTxId:" + firstTxId +
            ", lastTxId:" + lastTxId +
            ", version:" + version +
            ", completionTime:" + completionTime +
            ", ledgerSequenceNumber:" + ledgerSequenceNumber +
            ", lastEntryId:" + lastEntryId +
            ", lastSlotId:" + lastSlotId + "]";
    }
}
