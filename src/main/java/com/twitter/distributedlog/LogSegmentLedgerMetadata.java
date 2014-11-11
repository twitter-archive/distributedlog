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

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.exceptions.DLInterruptedException;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing the metadata associated
 * with a single edit log segment, stored in a single ledger
 */
public class LogSegmentLedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LogSegmentLedgerMetadata.class);

    public static enum TruncationStatus {
        ACTIVE (0), PARTIALLY_TRUNCATED(1), TRUNCATED (2);
        private final int value;

        private TruncationStatus(int value) {
            this.value = value;
        }
    }

    public static class LogSegmentLedgerMetadataBuilder {
        protected String zkPath;
        protected long ledgerId;
        protected int version;
        protected long firstTxId;
        protected int regionId = DistributedLogConstants.LOCAL_REGION_ID;
        protected long status = DistributedLogConstants.LOGSEGMENT_DEFAULT_STATUS;
        protected long lastTxId = DistributedLogConstants.INVALID_TXID;
        protected long completionTime = 0;
        protected int recordCount = 0;
        protected long ledgerSequenceNo;
        protected long lastEntryId = -1;
        protected long lastSlotId = -1;
        protected boolean inprogress = true;

        LogSegmentLedgerMetadataBuilder(String zkPath,
                                        int version,
                                        long ledgerId,
                                        long firstTxId) {
            this.zkPath = zkPath;
            this.version = version;
            this.ledgerId = ledgerId;
            this.firstTxId = firstTxId;
        }

        public LogSegmentLedgerMetadataBuilder setZkPath(String zkPath) {
            this.zkPath = zkPath;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setLedgerId(long ledgerId) {
            this.ledgerId = ledgerId;
            return this;
        }

        public LogSegmentLedgerMetadataBuilder setVersion(int version) {
            this.version = version;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setFirstTxId(long firstTxId) {
            this.firstTxId = firstTxId;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setRegionId(int regionId) {
            this.regionId = regionId;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setStatus(long status) {
            this.status = status;
            return this;
        }

        public LogSegmentLedgerMetadataBuilder setLastTxId(long lastTxId) {
            this.lastTxId = lastTxId;
            return this;
        }

        public LogSegmentLedgerMetadataBuilder setCompletionTime(long completionTime) {
            this.completionTime = completionTime;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setRecordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public LogSegmentLedgerMetadataBuilder setRecordCount(LogRecord record) {
            this.recordCount = record.getPositionWithinLogSegment();
            return this;
        }

        public LogSegmentLedgerMetadataBuilder setInprogress(boolean inprogress) {
            this.inprogress = inprogress;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setLedgerSequenceNo(long ledgerSequenceNo) {
            this.ledgerSequenceNo = ledgerSequenceNo;
            return this;
        }

        public LogSegmentLedgerMetadataBuilder setLastEntryId(long lastEntryId) {
            this.lastEntryId = lastEntryId;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setLastSlotId(long lastSlotId) {
            this.lastSlotId = lastSlotId;
            return this;
        }

        public LogSegmentLedgerMetadata build() {
            return new LogSegmentLedgerMetadata(
                zkPath,
                version,
                ledgerId,
                firstTxId,
                lastTxId,
                completionTime,
                inprogress,
                recordCount,
                ledgerSequenceNo,
                lastEntryId,
                lastSlotId,
                regionId,
                status
            );
        }

    }


    /**
     * Mutator to mutate the metadata of a log segment. This mutator is going to create
     * a new instance of the log segment metadata without changing the existing one.
     */
    public static class Mutator extends LogSegmentLedgerMetadataBuilder {

        Mutator(LogSegmentLedgerMetadata original) {
            super(original.getZkPath(), original.getVersion(), original.getLedgerId(), original.getFirstTxId());
            this.inprogress = original.isInProgress();
            this.ledgerSequenceNo = original.getLedgerSequenceNumber();
            this.lastEntryId = original.getLastEntryId();
            this.lastSlotId = original.getLastSlotId();
            this.lastTxId = original.getLastTxId();
            this.completionTime = original.getCompletionTime();
            this.recordCount = original.getRecordCount();
            this.regionId = original.getRegionId();
            this.status = original.getStatus();
        }

        public Mutator setLedgerSequenceNumber(long seqNo) {
            this.ledgerSequenceNo = seqNo;
            return this;
        }

        public Mutator setZkPath(String zkPath) {
            this.zkPath = zkPath;
            return this;
        }

        public Mutator setLastDLSN(DLSN dlsn) {
            this.ledgerSequenceNo = dlsn.getLedgerSequenceNo();
            this.lastEntryId = dlsn.getEntryId();
            this.lastSlotId = dlsn.getSlotId();
            return this;
        }

        public Mutator setTruncationStatus(TruncationStatus truncationStatus) {
            status &= ~METADATA_TRUNCATION_STATUS_MASK;
            status |= (truncationStatus.value & METADATA_TRUNCATION_STATUS_MASK);
            return this;
        }
    }

    private String zkPath;
    private final long ledgerId;
    private final int version;
    private final long firstTxId;
    private final int regionId;
    private long status;
    private long lastTxId;
    private long completionTime;
    private int recordCount;
    private DLSN lastDLSN = DLSN.InvalidDLSN;
    private boolean inprogress;

    public static final Comparator COMPARATOR
        = new Comparator<LogSegmentLedgerMetadata>() {

        public int compare(LogSegmentLedgerMetadata o1,
                           LogSegmentLedgerMetadata o2) {
            if ((o1.getLedgerSequenceNumber() == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO) ||
                (o2.getLedgerSequenceNumber() == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO)) {
                if (o1.firstTxId < o2.firstTxId) {
                    return -1;
                } else if (o1.firstTxId == o2.firstTxId) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (o1.getLedgerSequenceNumber() < o2.getLedgerSequenceNumber()) {
                    return -1;
                } else if (o1.getLedgerSequenceNumber() == o2.getLedgerSequenceNumber()) {
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
            if ((o1.getLedgerSequenceNumber() == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO) ||
                (o2.getLedgerSequenceNumber() == DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO)) {
                if (o1.firstTxId > o2.firstTxId) {
                    return -1;
                } else if (o1.firstTxId == o2.firstTxId) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (o1.getLedgerSequenceNumber() > o2.getLedgerSequenceNumber()) {
                    return -1;
                } else if (o1.getLedgerSequenceNumber() == o2.getLedgerSequenceNumber()) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }
    };

    static final int LOGRECORD_COUNT_SHIFT = 32;
    static final long LOGRECORD_COUNT_MASK = 0xffffffff00000000L;
    static final int REGION_SHIFT = 28;
    static final long MAX_REGION_ID = 0xfL;
    static final long REGION_MASK = 0x00000000f0000000L;
    static final int STATUS_BITS_SHIFT = 8;
    static final long STATUS_BITS_MASK = 0x000000000000ff00L;
    static final long UNUSED_BITS_MASK = 0x000000000fff0000L;
    static final long METADATA_VERSION_MASK = 0x00000000000000ffL;

    //Metadata status bits
    static final long METADATA_TRUNCATION_STATUS_MASK = 0x3L;
    static final long METADATA_STATUS_BIT_MAX = 0xffL;

    private LogSegmentLedgerMetadata(String zkPath,
                                     int version,
                                     long ledgerId,
           long firstTxId, long lastTxId, long completionTime, boolean inprogress, int recordCount,
           long ledgerSequenceNumber, long lastEntryId, long lastSlotId, int regionId, long status) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = lastTxId;
        this.inprogress = inprogress;
        this.completionTime = completionTime;
        this.recordCount = recordCount;
        this.lastDLSN = new DLSN(ledgerSequenceNumber, lastEntryId, lastSlotId);
        this.regionId = regionId;
        this.status = status;
    }

    public String getZkPath() {
        return zkPath;
    }

    public String getZNodeName() {
        return new File(zkPath).getName();
    }

    public long getFirstTxId() {
        return firstTxId;
    }

    public long getLastTxId() {
        return lastTxId;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getLedgerSequenceNumber() {
        return lastDLSN.getLedgerSequenceNo();
    }

    int getVersion() {
        return version;
    }

    public long getLastEntryId() {
        return lastDLSN.getEntryId();
    }

    long getStatus() {
        return status;
    }

    public boolean isTruncated() {
        return ((status & METADATA_TRUNCATION_STATUS_MASK)
                == TruncationStatus.TRUNCATED.value);
    }

    public boolean isPartiallyTruncated() {
        return ((status & METADATA_TRUNCATION_STATUS_MASK)
                == TruncationStatus.PARTIALLY_TRUNCATED.value);
    }

    @VisibleForTesting
    public LogSegmentLedgerMetadata setLastEntryId(long entryId) {
        lastDLSN = new DLSN(lastDLSN.getLedgerSequenceNo(), entryId, lastDLSN.getSlotId());
        return this;
    }

    public long getLastSlotId() {
        return lastDLSN.getSlotId();
    }

    public DLSN getLastDLSN() {
        return lastDLSN;
    }

    public DLSN getFirstDLSN() {
        return new DLSN(getLedgerSequenceNumber(), 0, 0);
    }

    public int getRecordCount() {
        return recordCount;
    }

    public int getRegionId() {
        return regionId;
    }

    public boolean isInProgress() {
        return this.inprogress;
    }

    @VisibleForTesting
    public boolean isDLSNinThisSegment(DLSN dlsn) {
        return dlsn.getLedgerSequenceNo() == getLedgerSequenceNumber();
    }

    @VisibleForTesting
    public boolean isRecordPositionWithinSegmentScope(LogRecord record) {
        return record.getPositionWithinLogSegment() <= getRecordCount();
    }

    @VisibleForTesting
    public boolean isRecordLastPositioninThisSegment(LogRecord record) {
        return record.getPositionWithinLogSegment() == getRecordCount();
    }

    long finalizeLedger(long newLastTxId, int recordCount, long lastEntryId, long lastSlotId) {
        assert this.lastTxId == DistributedLogConstants.INVALID_TXID;
        this.lastTxId = newLastTxId;
        this.lastDLSN = new DLSN(this.lastDLSN.getLedgerSequenceNo(), lastEntryId, lastSlotId);
        this.inprogress = false;
        this.completionTime = Utils.nowInMillis();
        this.recordCount = recordCount;
        return this.completionTime;
    }

    public static LogSegmentLedgerMetadata read(ZooKeeperClient zkc, String path, int targetVersion)
        throws IOException, KeeperException.NoNodeException {
        try {
            Stat stat = new Stat();
            byte[] data = zkc.get().getData(path, false, stat);
            LogSegmentLedgerMetadata metadata = parseData(path, data, targetVersion);
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

    public static void read(ZooKeeperClient zkc, String path, final int targetVersion,
                            final BookkeeperInternalCallbacks.GenericCallback<LogSegmentLedgerMetadata> callback) {
        try {
            zkc.get().getData(path, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        callback.operationComplete(rc, null);
                        return;
                    }
                    try {
                        LogSegmentLedgerMetadata metadata = parseData(path, data, targetVersion);
                        callback.operationComplete(KeeperException.Code.OK.intValue(), metadata);
                    } catch (IOException ie) {
                        // as we don't have return code for distributedlog. for now, we leveraged zk
                        // exception code.
                        callback.operationComplete(KeeperException.Code.BADARGUMENTS.intValue(), null);
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            callback.operationComplete(KeeperException.Code.OPERATIONTIMEOUT.intValue(), null);
        } catch (InterruptedException e) {
            callback.operationComplete(DistributedLogConstants.DL_INTERRUPTED_EXCEPTION_RESULT_CODE, null);
        }
    }

    static LogSegmentLedgerMetadata parseDataV1(String path, byte[] data, String[] parts, int targetVersion)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (1 == version);

        int regionId = (int)(versionStatusCount & REGION_MASK) >> REGION_SHIFT;
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 3) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            return new LogSegmentLedgerMetadataBuilder(path, targetVersion, ledgerId, txId)
                    .setRegionId(regionId)
                    .setStatus(status)
                    .build();
        } else if (parts.length == 5) {
            long recordCount = (versionStatusCount & LOGRECORD_COUNT_MASK) >> LOGRECORD_COUNT_SHIFT;
            assert (recordCount >= Integer.MIN_VALUE && recordCount <= Integer.MAX_VALUE);

            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            return new LogSegmentLedgerMetadataBuilder(path, targetVersion, ledgerId, firstTxId)
                .setInprogress(false)
                .setLastTxId(lastTxId)
                .setCompletionTime(completionTime)
                .setRecordCount((int)recordCount)
                .setRegionId(regionId)
                .setStatus(status)
                .build();
        } else {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }
    }

    static LogSegmentLedgerMetadata parseDataLatestVersion(String path, byte[] data, String[] parts, int targetVersion)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION == version);

        int regionId = (int)((versionStatusCount & REGION_MASK) >> REGION_SHIFT);
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 4) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long ledgerSequenceNumber = Long.valueOf(parts[3]);
            return new LogSegmentLedgerMetadataBuilder(path, targetVersion, ledgerId, txId)
                .setLedgerSequenceNo(ledgerSequenceNumber)
                .setRegionId(regionId)
                .setStatus(status)
                .build();
        } else if (parts.length == 8) {
            long recordCount = (versionStatusCount & LOGRECORD_COUNT_MASK) >> LOGRECORD_COUNT_SHIFT;
            assert (recordCount >= Integer.MIN_VALUE && recordCount <= Integer.MAX_VALUE);

            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            long ledgerSequenceNumber = Long.valueOf(parts[5]);
            long lastEntryId = Long.valueOf(parts[6]);
            long lastSlotId = Long.valueOf(parts[7]);
            return new LogSegmentLedgerMetadataBuilder(path, targetVersion, ledgerId, firstTxId)
                .setInprogress(false)
                .setLastTxId(lastTxId)
                .setCompletionTime(completionTime)
                .setRecordCount((int) recordCount)
                .setLedgerSequenceNo(ledgerSequenceNumber)
                .setLastEntryId(lastEntryId)
                .setLastSlotId(lastSlotId)
                .setRegionId(regionId)
                .setStatus(status)
                .build();
        } else {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }

    }

    static LogSegmentLedgerMetadata parseData(String path, byte[] data, int targetVersion) throws IOException {
        String[] parts = new String(data, UTF_8).split(";");
        long version;
        try {
            version = Long.valueOf(parts[0]) & METADATA_VERSION_MASK;
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

    public String getFinalisedData() {
        return getFinalisedData(version, inprogress, ledgerId, firstTxId, lastTxId,
                                getLedgerSequenceNumber(), getLastEntryId(), getLastSlotId(),
                                regionId, recordCount, completionTime, status);
    }

    protected static String getFinalisedData(int version, boolean inprogress,
                                             long ledgerId, long firstTxId, long lastTxId,
                                             long ledgerSeqNo, long lastEntryId, long lastSlotId,
                                             int regionId, int recordCount,
                                             long completionTime, long status) {
        String finalisedData;

        long versionStatusCount = ((long) version);
        if (1 == version) {
            if (inprogress) {
                finalisedData = String.format("%d;%d;%d",
                    version, ledgerId, firstTxId);
            } else {
                long versionAndCount = ((long) version) | ((long)recordCount << LOGRECORD_COUNT_SHIFT);
                finalisedData = String.format("%d;%d;%d;%d;%d",
                    versionAndCount, ledgerId, firstTxId, lastTxId, completionTime);
            }
        } else {
            versionStatusCount |= ((status & METADATA_STATUS_BIT_MAX) << STATUS_BITS_SHIFT);
            versionStatusCount |= (((long) regionId & MAX_REGION_ID) << REGION_SHIFT);
            if (!inprogress) {
                versionStatusCount |= ((long)recordCount << LOGRECORD_COUNT_SHIFT);
            }
            assert (DistributedLogConstants.LEDGER_METADATA_CURRENT_LAYOUT_VERSION == version);
            if (inprogress) {
                finalisedData = String.format("%d;%d;%d;%d",
                    versionStatusCount, ledgerId, firstTxId, ledgerSeqNo);
            } else {
                finalisedData = String.format("%d;%d;%d;%d;%d;%d;%d;%d",
                    versionStatusCount, ledgerId, firstTxId, lastTxId, completionTime,
                    ledgerSeqNo, lastEntryId, lastSlotId);
            }
        }
        return finalisedData;
    }

    void setZkPath(String path) {
        this.zkPath = path;
    }

    public void write(ZooKeeperClient zkc, String path)
        throws IOException, KeeperException.NodeExistsException {
        this.zkPath = path;
        String finalisedData = getFinalisedData();
        try {
            zkc.get().create(path, finalisedData.getBytes(UTF_8),
                zkc.getDefaultACL(), CreateMode.PERSISTENT);
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
            LogSegmentLedgerMetadata other = read(zkc, path, version);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Verifying {} against {}", this, other);
            }

            boolean retVal;

            // All fields may not be comparable so only compare the ones
            // that can be compared
            // completionTime is set when a node is finalized, so that
            // cannot be compared
            // if the node is inprogress, don't compare the lastTxId either
            if (this.getLedgerSequenceNumber() != other.getLedgerSequenceNumber() ||
                this.ledgerId != other.ledgerId ||
                this.firstTxId != other.firstTxId) {
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
        return getLedgerSequenceNumber() == ol.getLedgerSequenceNumber()
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
        hash = hash * 31 + version;
        hash = hash * 31 + (int) completionTime;
        hash = hash * 31 + (int) getLedgerSequenceNumber();
        return hash;
    }

    public String toString() {
        return "[LedgerId:" + ledgerId +
            ", firstTxId:" + firstTxId +
            ", lastTxId:" + lastTxId +
            ", version:" + version +
            ", completionTime:" + completionTime +
            ", recordCount:" + recordCount +
            ", regionId:" + regionId +
            ", status:" + status +
            ", ledgerSequenceNumber:" + getLedgerSequenceNumber() +
            ", lastEntryId:" + getLastEntryId() +
            ", lastSlotId:" + getLastSlotId() + "]";
    }

    public Mutator mutator() {
        return new Mutator(this);
    }
}
