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
import java.util.Comparator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private boolean inprogress;

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


    LogSegmentLedgerMetadata(String zkPath, int version,
                             long ledgerId, long firstTxId) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = DistributedLogConstants.INVALID_TXID;
        this.inprogress = true;
    }

    LogSegmentLedgerMetadata(String zkPath, int version, long ledgerId,
                             long firstTxId, long lastTxId, long completionTime) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = lastTxId;
        this.inprogress = false;
        this.completionTime = completionTime;
    }

    String getZkPath() {
        return zkPath;
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

    int getVersion() {
        return version;
    }

    boolean isInProgress() {
        return this.inprogress;
    }

    long finalizeLedger(long newLastTxId) {
        assert this.lastTxId == DistributedLogConstants.INVALID_TXID;
        this.lastTxId = newLastTxId;
        this.inprogress = false;
        this.completionTime = Utils.nowInMillis();
        return this.completionTime;
    }

    static LogSegmentLedgerMetadata read(ZooKeeperClient zkc, String path)
        throws IOException, KeeperException.NoNodeException {
        try {
            byte[] data = zkc.get().getData(path, false, null);
            String[] parts = new String(data).split(";");
            if (parts.length == 3) {
                int version = Integer.valueOf(parts[0]);
                long ledgerId = Long.valueOf(parts[1]);
                long txId = Long.valueOf(parts[2]);
                return new LogSegmentLedgerMetadata(path, version, ledgerId, txId);
            } else if (parts.length == 5) {
                int version = Integer.valueOf(parts[0]);
                long ledgerId = Long.valueOf(parts[1]);
                long firstTxId = Long.valueOf(parts[2]);
                long lastTxId = Long.valueOf(parts[3]);
                long completionTime = Long.valueOf(parts[4]);
                return new LogSegmentLedgerMetadata(path, version, ledgerId,
                    firstTxId, lastTxId, completionTime);
            } else {
                throw new IOException("Invalid ledger entry, "
                    + new String(data));
            }
        } catch (KeeperException.NoNodeException nne) {
            throw nne;
        } catch (Exception e) {
            throw new IOException("Error reading from zookeeper", e);
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
            finalisedData = String.format("%d;%d;%d;%d;%d",
                version, ledgerId, firstTxId, lastTxId, completionTime);
        }
        try {
            zkc.get().create(path, finalisedData.getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            throw nee;
        } catch (Exception e) {
            LOG.error("Error creating ledger znode {}", path, e);
            throw new IOException("Error creating ledger znode" + path);
        }
    }

    boolean verify(ZooKeeperClient zkc, String path) {
        try {
            LogSegmentLedgerMetadata other = read(zkc, path);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Verifying " + this.toString()
                    + " against " + other);
            }
            return other == this;
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
        hash = hash * 31 + (int) version;
        hash = hash * 31 + (int) completionTime;
        return hash;
    }

    public String toString() {
        return "[LedgerId:" + ledgerId +
            ", firstTxId:" + firstTxId +
            ", lastTxId:" + lastTxId +
            ", version:" + version +
            ", completionTime:" + completionTime + "]";
    }
}
