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
package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import org.apache.bookkeeper.client.BKException;
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

import com.google.common.annotations.VisibleForTesting;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing the metadata associated
 * with a single edit log segment, stored in a single ledger
 */
class LogSegmentLedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LogSegmentLedgerMetadata.class);

    public static enum LogSegmentLedgerMetadataVersion {
        VERSION_INVALID(0),
        VERSION_V1_ORIGINAL(1),
        VERSION_V2_LEDGER_SEQNO(2),
        VERSION_V3_MIN_ACTIVE_DLSN(3),
        VERSION_V4_ENVELOPED_ENTRIES(4),
        VERSION_V5_SEQUENCE_ID(5);

        public final int value;

        private LogSegmentLedgerMetadataVersion(int value) {
            this.value = value;
        }

        public static LogSegmentLedgerMetadataVersion of(int version) {
            switch (version) {
                case 5:
                    return VERSION_V5_SEQUENCE_ID;
                case 4:
                    return VERSION_V4_ENVELOPED_ENTRIES;
                case 3:
                    return VERSION_V3_MIN_ACTIVE_DLSN;
                case 2:
                    return VERSION_V2_LEDGER_SEQNO;
                case 1:
                    return VERSION_V1_ORIGINAL;
                case 0:
                    return VERSION_INVALID;
                default:
                    throw new IllegalArgumentException("unknown version " + version);
            }
        }
    }

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

    public static class LogSegmentLedgerMetadataBuilder {
        protected String zkPath;
        protected long ledgerId;
        protected LogSegmentLedgerMetadataVersion version;
        protected long firstTxId;
        protected int regionId;
        protected long status;
        protected long lastTxId;
        protected long completionTime;
        protected int recordCount;
        protected long ledgerSequenceNo;
        protected long lastEntryId;
        protected long lastSlotId;
        protected long minActiveEntryId;
        protected long minActiveSlotId;
        protected long startSequenceId;
        protected boolean inprogress;

        // This is a derived attribute.
        // Since we overwrite the original version with the target version, information that is
        // derived from the original version (e.g. does it support enveloping of entries)
        // is lost while parsing.
        // NOTE: This value is not stored in the Metadata store.
        protected boolean envelopeEntries = false;

        LogSegmentLedgerMetadataBuilder(String zkPath,
                                        LogSegmentLedgerMetadataVersion version,
                                        long ledgerId,
                                        long firstTxId) {
            initialize();
            this.zkPath = zkPath;
            this.version = version;
            this.ledgerId = ledgerId;
            this.firstTxId = firstTxId;
        }

        LogSegmentLedgerMetadataBuilder(String zkPath,
                                        int version,
                                        long ledgerId,
                                        long firstTxId) {
            this(zkPath, LogSegmentLedgerMetadataVersion.values()[version], ledgerId, firstTxId);
        }

        private void initialize() {
            lastTxId = DistributedLogConstants.INVALID_TXID;
            completionTime = 0;
            recordCount = 0;
            lastEntryId = -1;
            lastSlotId = -1;
            minActiveEntryId = 0;
            minActiveSlotId = 0;
            inprogress = true;
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
            this.recordCount = record.getCount();
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

        LogSegmentLedgerMetadataBuilder setEnvelopeEntries(boolean envelopeEntries) {
            this.envelopeEntries = envelopeEntries;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setMinActiveEntryId(long minActiveEntryId) {
            this.minActiveEntryId = minActiveEntryId;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setMinActiveSlotId(long minActiveSlotId) {
            this.minActiveSlotId = minActiveSlotId;
            return this;
        }

        LogSegmentLedgerMetadataBuilder setStartSequenceId(long startSequenceId) {
            this.startSequenceId = startSequenceId;
            return this;
        }

        public LogSegmentLedgerMetadata build() {
            return new LogSegmentLedgerMetadata(
                zkPath,
                version.value,
                ledgerId,
                firstTxId,
                lastTxId,
                completionTime,
                inprogress,
                recordCount);
        }

    }


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

    public static final int LEDGER_METADATA_MAX_SUPPORTED_LAYOUT_VERSION =
        LogSegmentLedgerMetadataVersion.VERSION_V5_SEQUENCE_ID.value;

    static final int LOGRECORD_COUNT_SHIFT = 32;
    static final long LOGRECORD_COUNT_MASK = 0xffffffff00000000L;
    static final int REGION_SHIFT = 28;
    static final long REGION_MASK = 0x00000000f0000000L;
    static final int STATUS_BITS_SHIFT = 8;
    static final long STATUS_BITS_MASK = 0x000000000000ff00L;
    static final long UNUSED_BITS_MASK = 0x000000000fff0000L;
    static final long METADATA_VERSION_MASK = 0x00000000000000ffL;

    //Metadata status bits
    static final long METADATA_TRUNCATION_STATUS_MASK = 0x3L;
    static final long METADATA_STATUS_BIT_MAX = 0xffL;

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
                             long firstTxId, long lastTxId, long completionTime, boolean inprogress, int recordCount) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = lastTxId;
        this.inprogress = inprogress;
        this.completionTime = completionTime;
        this.recordCount = recordCount;
    }

    String getZkPath() {
        return zkPath;
    }

    String getZNodeName() {
        return new File(zkPath).getName();
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

    public boolean getEnvelopeEntries() {
        return supportsEnvelopedEntries(version);
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
        long version;
        try {
            version = Long.valueOf(parts[0]) & METADATA_VERSION_MASK;
        } catch (Exception exc) {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }

        if (version > LogSegmentLedgerMetadataVersion.VERSION_V1_ORIGINAL.value) {
            return parseDataHigherVersion(path, data, version, parts);
        }

        if (parts.length == 3) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            return new LogSegmentLedgerMetadata(path, (int)version, ledgerId, txId);
        } else if (parts.length == 5) {
            long versionAndCount = Long.valueOf(parts[0]);

            assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);

            long recordCount = (versionAndCount & LOGRECORD_COUNT_MASK) >> LOGRECORD_COUNT_SHIFT;
            assert (recordCount >= Integer.MIN_VALUE && recordCount <= Integer.MAX_VALUE);

            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            return new LogSegmentLedgerMetadata(path, (int)version, ledgerId,
                firstTxId, lastTxId, completionTime, false, (int)recordCount);
        } else {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }
    }


    static LogSegmentLedgerMetadata parseDataHigherVersion(String path, byte[] data, long version, String[] parts) throws IOException {
        if (version > LogSegmentLedgerMetadata.LEDGER_METADATA_MAX_SUPPORTED_LAYOUT_VERSION) {
            throw new IOException("Unsupported log segment ledger metadata version '" + version + "' : "
                + new String(data, UTF_8));
        }

        if (LogSegmentLedgerMetadataVersion.VERSION_V2_LEDGER_SEQNO.value == version) {
            return parseDataV2(path, data, parts);
        } else if (LogSegmentLedgerMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value >= version &&
            LogSegmentLedgerMetadataVersion.VERSION_V3_MIN_ACTIVE_DLSN.value <= version) {
            return parseDataVersionsWithMinActiveDLSN(path, data, parts);
        } else if (LogSegmentLedgerMetadataVersion.VERSION_V5_SEQUENCE_ID.value >= version) {
            return parseDataVersionsWithSequenceId(path, data, parts);
        }

        throw new IOException("Unsupported log segment ledger metadata version '" + version + "' : "
            + new String(data, UTF_8));
    }

    static LogSegmentLedgerMetadata parseDataV2(String path, byte[] data, String[] parts)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (2 == version);

        LogSegmentLedgerMetadataVersion llmv = LogSegmentLedgerMetadataVersion.VERSION_V2_LEDGER_SEQNO;

        int regionId = (int)((versionStatusCount & REGION_MASK) >> REGION_SHIFT);
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 4) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long ledgerSequenceNumber = Long.valueOf(parts[3]);
            return new LogSegmentLedgerMetadataBuilder(path, llmv, ledgerId, txId)
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
            return new LogSegmentLedgerMetadataBuilder(path, llmv, ledgerId, firstTxId)
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
            throw new IOException("Invalid logsegment metadata : "
                + new String(data, UTF_8));
        }

    }

    static LogSegmentLedgerMetadata parseDataVersionsWithMinActiveDLSN(String path, byte[] data, String[] parts)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (LogSegmentLedgerMetadataVersion.VERSION_V3_MIN_ACTIVE_DLSN.value <= version &&
            LogSegmentLedgerMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value >= version);

        LogSegmentLedgerMetadataVersion llmv = LogSegmentLedgerMetadataVersion.of((int) version);

        int regionId = (int)((versionStatusCount & REGION_MASK) >> REGION_SHIFT);
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 6) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long ledgerSequenceNumber = Long.valueOf(parts[3]);
            long minActiveEntryId = Long.valueOf(parts[4]);
            long minActiveSlotId = Long.valueOf(parts[5]);

            LogSegmentLedgerMetadataBuilder builder = new LogSegmentLedgerMetadataBuilder(path, llmv, ledgerId, txId)
                .setLedgerSequenceNo(ledgerSequenceNumber)
                .setMinActiveEntryId(minActiveEntryId)
                .setMinActiveSlotId(minActiveSlotId)
                .setRegionId(regionId)
                .setStatus(status);
            if (supportsEnvelopedEntries((int) version)) {
                builder = builder.setEnvelopeEntries(true);
            }
            return builder.build();
        } else if (parts.length == 10) {
            long recordCount = (versionStatusCount & LOGRECORD_COUNT_MASK) >> LOGRECORD_COUNT_SHIFT;
            assert (recordCount >= Integer.MIN_VALUE && recordCount <= Integer.MAX_VALUE);

            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            long ledgerSequenceNumber = Long.valueOf(parts[5]);
            long lastEntryId = Long.valueOf(parts[6]);
            long lastSlotId = Long.valueOf(parts[7]);
            long minActiveEntryId = Long.valueOf(parts[8]);
            long minActiveSlotId = Long.valueOf(parts[9]);
            LogSegmentLedgerMetadataBuilder builder = new LogSegmentLedgerMetadataBuilder(path, llmv, ledgerId, firstTxId)
                .setInprogress(false)
                .setLastTxId(lastTxId)
                .setCompletionTime(completionTime)
                .setRecordCount((int) recordCount)
                .setLedgerSequenceNo(ledgerSequenceNumber)
                .setLastEntryId(lastEntryId)
                .setLastSlotId(lastSlotId)
                .setMinActiveEntryId(minActiveEntryId)
                .setMinActiveSlotId(minActiveSlotId)
                .setRegionId(regionId)
                .setStatus(status);
            if (supportsEnvelopedEntries((int) version)) {
                builder = builder.setEnvelopeEntries(true);
            }
            return builder.build();
        } else {
            throw new IOException("Invalid logsegment metadata : "
                + new String(data, UTF_8));
        }

    }

    static LogSegmentLedgerMetadata parseDataVersionsWithSequenceId(String path, byte[] data, String[] parts)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (LogSegmentLedgerMetadataVersion.VERSION_V5_SEQUENCE_ID.value <= version &&
            LogSegmentLedgerMetadata.LEDGER_METADATA_MAX_SUPPORTED_LAYOUT_VERSION >= version);

        LogSegmentLedgerMetadataVersion llmv = LogSegmentLedgerMetadataVersion.of((int) version);

        int regionId = (int)((versionStatusCount & REGION_MASK) >> REGION_SHIFT);
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 7) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long ledgerSequenceNumber = Long.valueOf(parts[3]);
            long minActiveEntryId = Long.valueOf(parts[4]);
            long minActiveSlotId = Long.valueOf(parts[5]);
            long startSequenceId = Long.valueOf(parts[6]);

            LogSegmentLedgerMetadataBuilder builder = new LogSegmentLedgerMetadataBuilder(path, llmv, ledgerId, txId)
                .setLedgerSequenceNo(ledgerSequenceNumber)
                .setMinActiveEntryId(minActiveEntryId)
                .setMinActiveSlotId(minActiveSlotId)
                .setRegionId(regionId)
                .setStatus(status)
                .setStartSequenceId(startSequenceId)
                .setEnvelopeEntries(true);
            return builder.build();
        } else if (parts.length == 11) {
            long recordCount = (versionStatusCount & LOGRECORD_COUNT_MASK) >> LOGRECORD_COUNT_SHIFT;
            assert (recordCount >= Integer.MIN_VALUE && recordCount <= Integer.MAX_VALUE);

            long ledgerId = Long.valueOf(parts[1]);
            long firstTxId = Long.valueOf(parts[2]);
            long lastTxId = Long.valueOf(parts[3]);
            long completionTime = Long.valueOf(parts[4]);
            long ledgerSequenceNumber = Long.valueOf(parts[5]);
            long lastEntryId = Long.valueOf(parts[6]);
            long lastSlotId = Long.valueOf(parts[7]);
            long minActiveEntryId = Long.valueOf(parts[8]);
            long minActiveSlotId = Long.valueOf(parts[9]);
            long startSequenceId = Long.valueOf(parts[10]);
            LogSegmentLedgerMetadataBuilder builder = new LogSegmentLedgerMetadataBuilder(path, llmv, ledgerId, firstTxId)
                .setInprogress(false)
                .setLastTxId(lastTxId)
                .setCompletionTime(completionTime)
                .setRecordCount((int) recordCount)
                .setLedgerSequenceNo(ledgerSequenceNumber)
                .setLastEntryId(lastEntryId)
                .setLastSlotId(lastSlotId)
                .setMinActiveEntryId(minActiveEntryId)
                .setMinActiveSlotId(minActiveSlotId)
                .setRegionId(regionId)
                .setStatus(status)
                .setStartSequenceId(startSequenceId)
                .setEnvelopeEntries(true);
            return builder.build();
        } else {
            throw new IOException("Invalid log segment metadata : "
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
                zkc.getDefaultACL(), CreateMode.PERSISTENT);
            zkVersion = 0;
        } catch (KeeperException.NoNodeException nne) {
            LOG.error("Error creating ledger znode {}", path, nne);
            throw new LogNotFoundException(String.format("Log %s does not exist or has been deleted", nne.getPath()));
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

    @VisibleForTesting
    void overwriteLastTxId(long lastTxId) {
        this.lastTxId = lastTxId;
    }

    public boolean supportsLedgerSequenceNo() {
        return supportsLedgerSequenceNo(version);
    }

    /**
     * Whether the provided version supports ledger sequence number.
     *
     * @param version
     *          log segment metadata version
     * @return true if this log segment supports ledger sequence number.
     */
    public static boolean supportsLedgerSequenceNo(int version) {
        return version >= LogSegmentLedgerMetadataVersion.VERSION_V2_LEDGER_SEQNO.value;
    }

    /**
     * Whether the provided version supports enveloping entries before writing to bookkeeper.
     *
     * @param version
     *          log segment metadata version
     * @return true if this log segment supports enveloping entries
     */
    public static boolean supportsEnvelopedEntries(int version) {
        return version >= LogSegmentLedgerMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value;
    }

    public boolean supportsSequenceId() {
        return supportsSequenceId(version);
    }

    /**
     * Whether the provided version supports sequence id.
     *
     * @param version
     *          log segment metadata version
     * @return true if the log segment support sequence id.
     */
    public static boolean supportsSequenceId(int version) {
        return version >= LogSegmentLedgerMetadataVersion.VERSION_V5_SEQUENCE_ID.value;
    }
}
