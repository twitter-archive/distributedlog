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

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.UnsupportedMetadataVersionException;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing the metadata associated
 * with a single edit log segment, stored in a single ledger
 */
public class LogSegmentMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LogSegmentMetadata.class);

    public static enum LogSegmentMetadataVersion {
        VERSION_INVALID(0),
        VERSION_V1_ORIGINAL(1),
        VERSION_V2_LEDGER_SEQNO(2),
        VERSION_V3_MIN_ACTIVE_DLSN(3),
        VERSION_V4_ENVELOPED_ENTRIES(4),
        VERSION_V5_SEQUENCE_ID(5);

        public final int value;

        private LogSegmentMetadataVersion(int value) {
            this.value = value;
        }

        public static LogSegmentMetadataVersion of(int version) {
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

    public static enum TruncationStatus {
        ACTIVE (0), PARTIALLY_TRUNCATED(1), TRUNCATED (2);
        private final int value;

        private TruncationStatus(int value) {
            this.value = value;
        }
    }

    public static class LogSegmentMetadataBuilder {
        protected String zkPath;
        protected long ledgerId;
        protected LogSegmentMetadataVersion version;
        protected long firstTxId;
        protected int regionId;
        protected long status;
        protected long lastTxId;
        protected long completionTime;
        protected int recordCount;
        protected long logSegmentSequenceNo;
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

        LogSegmentMetadataBuilder(String zkPath,
                                  LogSegmentMetadataVersion version,
                                  long ledgerId,
                                  long firstTxId) {
            initialize();
            this.zkPath = zkPath;
            this.version = version;
            this.ledgerId = ledgerId;
            this.firstTxId = firstTxId;
        }

        LogSegmentMetadataBuilder(String zkPath,
                                  int version,
                                  long ledgerId,
                                  long firstTxId) {
            this(zkPath, LogSegmentMetadataVersion.values()[version], ledgerId, firstTxId);
        }

        private void initialize() {
            regionId = DistributedLogConstants.LOCAL_REGION_ID;
            status = DistributedLogConstants.LOGSEGMENT_DEFAULT_STATUS;
            lastTxId = DistributedLogConstants.INVALID_TXID;
            completionTime = 0;
            recordCount = 0;
            lastEntryId = -1;
            lastSlotId = -1;
            minActiveEntryId = 0;
            minActiveSlotId = 0;
            startSequenceId = DistributedLogConstants.UNASSIGNED_SEQUENCE_ID;
            inprogress = true;
        }

        LogSegmentMetadataBuilder setRegionId(int regionId) {
            this.regionId = regionId;
            return this;
        }

        LogSegmentMetadataBuilder setStatus(long status) {
            this.status = status;
            return this;
        }

        public LogSegmentMetadataBuilder setLastTxId(long lastTxId) {
            this.lastTxId = lastTxId;
            return this;
        }

        public LogSegmentMetadataBuilder setCompletionTime(long completionTime) {
            this.completionTime = completionTime;
            return this;
        }

        LogSegmentMetadataBuilder setRecordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public LogSegmentMetadataBuilder setRecordCount(LogRecord record) {
            this.recordCount = record.getPositionWithinLogSegment();
            return this;
        }

        public LogSegmentMetadataBuilder setInprogress(boolean inprogress) {
            this.inprogress = inprogress;
            return this;
        }

        LogSegmentMetadataBuilder setLogSegmentSequenceNo(long logSegmentSequenceNo) {
            this.logSegmentSequenceNo = logSegmentSequenceNo;
            return this;
        }

        public LogSegmentMetadataBuilder setLastEntryId(long lastEntryId) {
            this.lastEntryId = lastEntryId;
            return this;
        }

        LogSegmentMetadataBuilder setLastSlotId(long lastSlotId) {
            this.lastSlotId = lastSlotId;
            return this;
        }

        LogSegmentMetadataBuilder setEnvelopeEntries(boolean envelopeEntries) {
            this.envelopeEntries = envelopeEntries;
            return this;
        }

        LogSegmentMetadataBuilder setMinActiveEntryId(long minActiveEntryId) {
            this.minActiveEntryId = minActiveEntryId;
            return this;
        }

        LogSegmentMetadataBuilder setMinActiveSlotId(long minActiveSlotId) {
            this.minActiveSlotId = minActiveSlotId;
            return this;
        }

        LogSegmentMetadataBuilder setStartSequenceId(long startSequenceId) {
            this.startSequenceId = startSequenceId;
            return this;
        }

        public LogSegmentMetadata build() {
            return new LogSegmentMetadata(
                zkPath,
                version,
                ledgerId,
                firstTxId,
                lastTxId,
                completionTime,
                inprogress,
                recordCount,
                    logSegmentSequenceNo,
                lastEntryId,
                lastSlotId,
                regionId,
                status,
                minActiveEntryId,
                minActiveSlotId,
                startSequenceId,
                envelopeEntries
            );
        }

    }

    /**
     * Mutator to mutate the metadata of a log segment. This mutator is going to create
     * a new instance of the log segment metadata without changing the existing one.
     */
    public static class Mutator extends LogSegmentMetadataBuilder {

        Mutator(LogSegmentMetadata original) {
            super(original.getZkPath(), original.getVersion(), original.getLedgerId(), original.getFirstTxId());
            this.inprogress = original.isInProgress();
            this.logSegmentSequenceNo = original.getLogSegmentSequenceNumber();
            this.lastEntryId = original.getLastEntryId();
            this.lastSlotId = original.getLastSlotId();
            this.lastTxId = original.getLastTxId();
            this.completionTime = original.getCompletionTime();
            this.recordCount = original.getRecordCount();
            this.regionId = original.getRegionId();
            this.status = original.getStatus();
            this.minActiveEntryId = original.getMinActiveDLSN().getEntryId();
            this.minActiveSlotId = original.getMinActiveDLSN().getSlotId();
            this.startSequenceId = original.getStartSequenceId();
            this.envelopeEntries = original.getEnvelopeEntries();
        }

        @VisibleForTesting
        public Mutator setVersion(LogSegmentMetadataVersion version) {
            this.version = version;
            return this;
        }

        public Mutator setLogSegmentSequenceNumber(long seqNo) {
            this.logSegmentSequenceNo = seqNo;
            return this;
        }

        public Mutator setZkPath(String zkPath) {
            this.zkPath = zkPath;
            return this;
        }

        public Mutator setLastDLSN(DLSN dlsn) {
            this.logSegmentSequenceNo = dlsn.getLogSegmentSequenceNo();
            this.lastEntryId = dlsn.getEntryId();
            this.lastSlotId = dlsn.getSlotId();
            return this;
        }

        public Mutator setMinActiveDLSN(DLSN dlsn) {
            if (this.logSegmentSequenceNo != dlsn.getLogSegmentSequenceNo()) {
                throw new IllegalArgumentException("Updating minDLSN in an incorrect log segment");
            }
            this.minActiveEntryId = dlsn.getEntryId();
            this.minActiveSlotId = dlsn.getSlotId();
            return this;
        }

        public Mutator setTruncationStatus(TruncationStatus truncationStatus) {
            status &= ~METADATA_TRUNCATION_STATUS_MASK;
            status |= (truncationStatus.value & METADATA_TRUNCATION_STATUS_MASK);
            return this;
        }

        public Mutator setStartSequenceId(long startSequenceId) {
            this.startSequenceId = startSequenceId;
            return this;
        }
    }

    private final String zkPath;
    private final long ledgerId;
    private final LogSegmentMetadataVersion version;
    private final long firstTxId;
    private final int regionId;
    private final long status;
    private final long lastTxId;
    private final long completionTime;
    private final int recordCount;
    private final DLSN lastDLSN;
    private final DLSN minActiveDLSN;
    private final long startSequenceId;
    private final boolean inprogress;
    // This is a derived attribute.
    // Since we overwrite the original version with the target version, information that is
    // derived from the original version (e.g. does it support enveloping of entries)
    // is lost while parsing.
    // NOTE: This value is not stored in the Metadata store.
    private final boolean envelopeEntries;

    public static final Comparator<LogSegmentMetadata> COMPARATOR
        = new Comparator<LogSegmentMetadata>() {

        public int compare(LogSegmentMetadata o1,
                           LogSegmentMetadata o2) {
            if ((o1.getLogSegmentSequenceNumber() == DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO) ||
                (o2.getLogSegmentSequenceNumber() == DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO)) {
                if (o1.firstTxId < o2.firstTxId) {
                    return -1;
                } else if (o1.firstTxId == o2.firstTxId) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (o1.getLogSegmentSequenceNumber() < o2.getLogSegmentSequenceNumber()) {
                    return -1;
                } else if (o1.getLogSegmentSequenceNumber() == o2.getLogSegmentSequenceNumber()) {
                    // make sure we won't move over inprogress log segment if it still presents in the list
                    if (o1.isInProgress() && !o2.isInProgress()) {
                        return -1;
                    } else if (!o1.isInProgress() && o2.isInProgress()) {
                        return 1;
                    } else {
                        return 0;
                    }
                } else {
                    return 1;
                }
            }


        }
    };

    public static final Comparator<LogSegmentMetadata> DESC_COMPARATOR
        = new Comparator<LogSegmentMetadata>() {
        public int compare(LogSegmentMetadata o1,
                           LogSegmentMetadata o2) {
            if ((o1.getLogSegmentSequenceNumber() == DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO) ||
                (o2.getLogSegmentSequenceNumber() == DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO)) {
                if (o1.firstTxId > o2.firstTxId) {
                    return -1;
                } else if (o1.firstTxId == o2.firstTxId) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (o1.getLogSegmentSequenceNumber() > o2.getLogSegmentSequenceNumber()) {
                    return -1;
                } else if (o1.getLogSegmentSequenceNumber() == o2.getLogSegmentSequenceNumber()) {
                    // make sure we won't move over inprogress log segment if it still presents in the list
                    if (o1.isInProgress() && !o2.isInProgress()) {
                        return 1;
                    } else if (!o1.isInProgress() && o2.isInProgress()) {
                        return -1;
                    } else {
                        return 0;
                    }
                } else {
                    return 1;
                }
            }
        }
    };

    public static final int LEDGER_METADATA_CURRENT_LAYOUT_VERSION =
                LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value;

    public static final int LEDGER_METADATA_OLDEST_SUPPORTED_VERSION =
        LogSegmentMetadataVersion.VERSION_V2_LEDGER_SEQNO.value;

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

    private LogSegmentMetadata(String zkPath,
                               LogSegmentMetadataVersion version,
                               long ledgerId,
                               long firstTxId,
                               long lastTxId,
                               long completionTime,
                               boolean inprogress,
                               int recordCount,
                               long logSegmentSequenceNumber,
                               long lastEntryId,
                               long lastSlotId,
                               int regionId,
                               long status,
                               long minActiveEntryId,
                               long minActiveSlotId,
                               long startSequenceId,
                               boolean envelopeEntries) {
        this.zkPath = zkPath;
        this.ledgerId = ledgerId;
        this.version = version;
        this.firstTxId = firstTxId;
        this.lastTxId = lastTxId;
        this.inprogress = inprogress;
        this.completionTime = completionTime;
        this.recordCount = recordCount;
        this.lastDLSN = new DLSN(logSegmentSequenceNumber, lastEntryId, lastSlotId);
        this.minActiveDLSN = new DLSN(logSegmentSequenceNumber, minActiveEntryId, minActiveSlotId);
        this.startSequenceId = startSequenceId;
        this.regionId = regionId;
        this.status = status;
        this.envelopeEntries = envelopeEntries;
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

    public long getLogSegmentSequenceNumber() {
        return lastDLSN.getLogSegmentSequenceNo();
    }

    public int getVersion() {
        return version.value;
    }

    public boolean getEnvelopeEntries() {
        return envelopeEntries;
    }

    public long getLastEntryId() {
        return lastDLSN.getEntryId();
    }

    long getStatus() {
        return status;
    }

    public long getStartSequenceId() {
        // generate negative sequence id for log segments that created <= v4
        return supportsSequenceId() && startSequenceId != DistributedLogConstants.UNASSIGNED_SEQUENCE_ID ?
                startSequenceId : Long.MIN_VALUE + (getLogSegmentSequenceNumber() << 32L);
    }

    public boolean isTruncated() {
        return ((status & METADATA_TRUNCATION_STATUS_MASK)
                == TruncationStatus.TRUNCATED.value);
    }

    public boolean isPartiallyTruncated() {
        return ((status & METADATA_TRUNCATION_STATUS_MASK)
                == TruncationStatus.PARTIALLY_TRUNCATED.value);
    }

    public boolean isNonTruncated() {
        return ((status & METADATA_TRUNCATION_STATUS_MASK)
                == TruncationStatus.ACTIVE.value);
    }

    public long getLastSlotId() {
        return lastDLSN.getSlotId();
    }

    public DLSN getLastDLSN() {
        return lastDLSN;
    }

    public DLSN getMinActiveDLSN() {
        return minActiveDLSN;
    }

    public DLSN getFirstDLSN() {
        return new DLSN(getLogSegmentSequenceNumber(), 0, 0);
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
        return dlsn.getLogSegmentSequenceNo() == getLogSegmentSequenceNumber();
    }

    @VisibleForTesting
    public boolean isRecordPositionWithinSegmentScope(LogRecord record) {
        return record.getPositionWithinLogSegment() <= getRecordCount();
    }

    @VisibleForTesting
    public boolean isRecordLastPositioninThisSegment(LogRecord record) {
        return record.getPositionWithinLogSegment() == getRecordCount();
    }

    /**
     * complete current log segment. A new log segment metadata instance will be returned.
     *
     * @param zkPath
     *          zk path for the completed log segment.
     * @param newLastTxId
     *          last tx id
     * @param recordCount
     *          record count
     * @param lastEntryId
     *          last entry id
     * @param lastSlotId
     *          last slot id
     * @return completed log segment.
     */
    LogSegmentMetadata completeLogSegment(String zkPath,
                                                long newLastTxId,
                                                int recordCount,
                                                long lastEntryId,
                                                long lastSlotId,
                                                long startSequenceId) {
        assert this.lastTxId == DistributedLogConstants.INVALID_TXID;

        return new Mutator(this)
                .setZkPath(zkPath)
                .setLastDLSN(new DLSN(this.lastDLSN.getLogSegmentSequenceNo(), lastEntryId, lastSlotId))
                .setLastTxId(newLastTxId)
                .setInprogress(false)
                .setCompletionTime(Utils.nowInMillis())
                .setRecordCount(recordCount)
                .setStartSequenceId(startSequenceId)
                .build();
    }

    public static Future<LogSegmentMetadata> read(ZooKeeperClient zkc, String path) {
        return read(zkc, path, false);
    }

    public static Future<LogSegmentMetadata> read(ZooKeeperClient zkc, String path, final boolean skipMinVersionCheck) {
        final Promise<LogSegmentMetadata> result = new Promise<LogSegmentMetadata>();
        try {
            zkc.get().getData(path, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        result.setException(KeeperException.create(KeeperException.Code.get(rc)));
                        return;
                    }
                    try {
                        LogSegmentMetadata metadata = parseData(path, data, skipMinVersionCheck);
                        result.setValue(metadata);
                    } catch (IOException ie) {
                        LOG.error("Error on parsing log segment metadata from {} : ", path, ie);
                        result.setException(ie);
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            result.setException(FutureUtils.zkException(e, path));
        } catch (InterruptedException e) {
            result.setException(FutureUtils.zkException(e, path));
        }
        return result;
    }

    static LogSegmentMetadata parseDataV1(String path, byte[] data, String[] parts)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (1 == version);

        LogSegmentMetadataVersion llmv = LogSegmentMetadataVersion.VERSION_V1_ORIGINAL;

        int regionId = (int)(versionStatusCount & REGION_MASK) >> REGION_SHIFT;
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 3) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            return new LogSegmentMetadataBuilder(path, llmv, ledgerId, txId)
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
            return new LogSegmentMetadataBuilder(path, llmv, ledgerId, firstTxId)
                .setInprogress(false)
                .setLastTxId(lastTxId)
                .setCompletionTime(completionTime)
                .setRecordCount((int) recordCount)
                .setRegionId(regionId)
                .setStatus(status)
                .build();
        } else {
            throw new IOException("Invalid log segment metadata : "
                + new String(data, UTF_8));
        }
    }

    static LogSegmentMetadata parseDataV2(String path, byte[] data, String[] parts)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (2 == version);

        LogSegmentMetadataVersion llmv = LogSegmentMetadataVersion.VERSION_V2_LEDGER_SEQNO;

        int regionId = (int)((versionStatusCount & REGION_MASK) >> REGION_SHIFT);
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 4) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long logSegmentSequenceNumber = Long.valueOf(parts[3]);
            return new LogSegmentMetadataBuilder(path, llmv, ledgerId, txId)
                .setLogSegmentSequenceNo(logSegmentSequenceNumber)
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
            long logSegmentSequenceNumber = Long.valueOf(parts[5]);
            long lastEntryId = Long.valueOf(parts[6]);
            long lastSlotId = Long.valueOf(parts[7]);
            return new LogSegmentMetadataBuilder(path, llmv, ledgerId, firstTxId)
                .setInprogress(false)
                .setLastTxId(lastTxId)
                .setCompletionTime(completionTime)
                .setRecordCount((int) recordCount)
                .setLogSegmentSequenceNo(logSegmentSequenceNumber)
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

    static LogSegmentMetadata parseDataVersionsWithMinActiveDLSN(String path, byte[] data, String[] parts)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (LogSegmentMetadataVersion.VERSION_V3_MIN_ACTIVE_DLSN.value <= version &&
                LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value >= version);

        LogSegmentMetadataVersion llmv = LogSegmentMetadataVersion.of((int) version);

        int regionId = (int)((versionStatusCount & REGION_MASK) >> REGION_SHIFT);
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 6) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long logSegmentSequenceNumber = Long.valueOf(parts[3]);
            long minActiveEntryId = Long.valueOf(parts[4]);
            long minActiveSlotId = Long.valueOf(parts[5]);

            LogSegmentMetadataBuilder builder = new LogSegmentMetadataBuilder(path, llmv, ledgerId, txId)
                .setLogSegmentSequenceNo(logSegmentSequenceNumber)
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
            long logSegmentSequenceNumber = Long.valueOf(parts[5]);
            long lastEntryId = Long.valueOf(parts[6]);
            long lastSlotId = Long.valueOf(parts[7]);
            long minActiveEntryId = Long.valueOf(parts[8]);
            long minActiveSlotId = Long.valueOf(parts[9]);
            LogSegmentMetadataBuilder builder = new LogSegmentMetadataBuilder(path, llmv, ledgerId, firstTxId)
                .setInprogress(false)
                .setLastTxId(lastTxId)
                .setCompletionTime(completionTime)
                .setRecordCount((int) recordCount)
                .setLogSegmentSequenceNo(logSegmentSequenceNumber)
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

    static LogSegmentMetadata parseDataVersionsWithSequenceId(String path, byte[] data, String[] parts)
        throws IOException {
        long versionStatusCount = Long.valueOf(parts[0]);

        long version = versionStatusCount & METADATA_VERSION_MASK;
        assert (version >= Integer.MIN_VALUE && version <= Integer.MAX_VALUE);
        assert (LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value <= version &&
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION >= version);

        LogSegmentMetadataVersion llmv = LogSegmentMetadataVersion.of((int) version);

        int regionId = (int)((versionStatusCount & REGION_MASK) >> REGION_SHIFT);
        assert (regionId >= 0 && regionId <= 0xf);

        long status = (versionStatusCount & STATUS_BITS_MASK) >> STATUS_BITS_SHIFT;
        assert (status >= 0 && status <= METADATA_STATUS_BIT_MAX);

        if (parts.length == 7) {
            long ledgerId = Long.valueOf(parts[1]);
            long txId = Long.valueOf(parts[2]);
            long logSegmentSequenceNumber = Long.valueOf(parts[3]);
            long minActiveEntryId = Long.valueOf(parts[4]);
            long minActiveSlotId = Long.valueOf(parts[5]);
            long startSequenceId = Long.valueOf(parts[6]);

            LogSegmentMetadataBuilder builder = new LogSegmentMetadataBuilder(path, llmv, ledgerId, txId)
                    .setLogSegmentSequenceNo(logSegmentSequenceNumber)
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
            long logSegmentSequenceNumber = Long.valueOf(parts[5]);
            long lastEntryId = Long.valueOf(parts[6]);
            long lastSlotId = Long.valueOf(parts[7]);
            long minActiveEntryId = Long.valueOf(parts[8]);
            long minActiveSlotId = Long.valueOf(parts[9]);
            long startSequenceId = Long.valueOf(parts[10]);
            LogSegmentMetadataBuilder builder = new LogSegmentMetadataBuilder(path, llmv, ledgerId, firstTxId)
                    .setInprogress(false)
                    .setLastTxId(lastTxId)
                    .setCompletionTime(completionTime)
                    .setRecordCount((int) recordCount)
                    .setLogSegmentSequenceNo(logSegmentSequenceNumber)
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

    static LogSegmentMetadata parseData(String path, byte[] data, boolean skipMinVersionCheck) throws IOException {
        String[] parts = new String(data, UTF_8).split(";");
        long version;
        try {
            version = Long.valueOf(parts[0]) & METADATA_VERSION_MASK;
        } catch (Exception exc) {
            throw new IOException("Invalid ledger entry, "
                + new String(data, UTF_8));
        }

        if (!skipMinVersionCheck && version < LogSegmentMetadata.LEDGER_METADATA_OLDEST_SUPPORTED_VERSION) {
            throw new UnsupportedMetadataVersionException("Ledger metadata version '" + version + "' is no longer supported: "
                + new String(data, UTF_8));
        }

        if (version > LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION) {
            throw new UnsupportedMetadataVersionException("Metadata version '" + version + "' is higher than the highest supported version : "
                + new String(data, UTF_8));
        }

        if (LogSegmentMetadataVersion.VERSION_V1_ORIGINAL.value == version) {
            return parseDataV1(path, data, parts);
        } else if (LogSegmentMetadataVersion.VERSION_V2_LEDGER_SEQNO.value == version) {
            return parseDataV2(path, data, parts);
        } else if (LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value >= version &&
                   LogSegmentMetadataVersion.VERSION_V3_MIN_ACTIVE_DLSN.value <= version) {
            return parseDataVersionsWithMinActiveDLSN(path, data, parts);
        } else {
            assert(version >= LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value);
            return parseDataVersionsWithSequenceId(path, data, parts);
        }
    }

    public String getFinalisedData() {
        return getFinalisedData(this.version);
    }

    public String getFinalisedData(LogSegmentMetadataVersion version) {
        String finalisedData;
        final long logSegmentSeqNo = getLogSegmentSequenceNumber();
        final long lastEntryId = getLastEntryId();
        final long lastSlotId = getLastSlotId();
        final long minActiveEntryId = minActiveDLSN.getEntryId();
        final long minActiveSlotId = minActiveDLSN.getSlotId();

        if (LogSegmentMetadataVersion.VERSION_V1_ORIGINAL == version) {
            if (inprogress) {
                finalisedData = String.format("%d;%d;%d",
                    version.value, ledgerId, firstTxId);
            } else {
                long versionAndCount = ((long) version.value) | ((long)recordCount << LOGRECORD_COUNT_SHIFT);
                finalisedData = String.format("%d;%d;%d;%d;%d",
                    versionAndCount, ledgerId, firstTxId, lastTxId, completionTime);
            }
        } else {
            long versionStatusCount = ((long) version.value);
            versionStatusCount |= ((status & METADATA_STATUS_BIT_MAX) << STATUS_BITS_SHIFT);
            versionStatusCount |= (((long) regionId & MAX_REGION_ID) << REGION_SHIFT);
            if (!inprogress) {
                versionStatusCount |= ((long)recordCount << LOGRECORD_COUNT_SHIFT);
            }
            if (LogSegmentMetadataVersion.VERSION_V2_LEDGER_SEQNO == version) {
                if (inprogress) {
                    finalisedData = String.format("%d;%d;%d;%d",
                        versionStatusCount, ledgerId, firstTxId, logSegmentSeqNo);
                } else {
                    finalisedData = String.format("%d;%d;%d;%d;%d;%d;%d;%d",
                        versionStatusCount, ledgerId, firstTxId, lastTxId, completionTime,
                        logSegmentSeqNo, lastEntryId, lastSlotId);
                }
            } else if (LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value >= version.value &&
                        LogSegmentMetadataVersion.VERSION_V3_MIN_ACTIVE_DLSN.value <= version.value) {
                if (inprogress) {
                    finalisedData = String.format("%d;%d;%d;%d;%d;%d",
                        versionStatusCount, ledgerId, firstTxId, logSegmentSeqNo, minActiveEntryId, minActiveSlotId);
                } else {
                    finalisedData = String.format("%d;%d;%d;%d;%d;%d;%d;%d;%d;%d",
                        versionStatusCount, ledgerId, firstTxId, lastTxId, completionTime,
                        logSegmentSeqNo, lastEntryId, lastSlotId, minActiveEntryId, minActiveSlotId);
                }
            } else if (LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value <= version.value &&
                        LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION >= version.value) {
                if (inprogress) {
                    finalisedData = String.format("%d;%d;%d;%d;%d;%d;%d",
                        versionStatusCount, ledgerId, firstTxId, logSegmentSeqNo, minActiveEntryId, minActiveSlotId, startSequenceId);
                } else {
                    finalisedData = String.format("%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d",
                        versionStatusCount, ledgerId, firstTxId, lastTxId, completionTime,
                        logSegmentSeqNo, lastEntryId, lastSlotId, minActiveEntryId, minActiveSlotId, startSequenceId);
                }
            } else {
                throw new IllegalStateException("Unsupported log segment ledger metadata version '" + version + "'");
            }
        }
        return finalisedData;
    }

    String getSegmentName() {
        String[] parts = this.zkPath.split("/");
        if (parts.length <= 0) {
            throw new IllegalStateException("ZK Path is not valid");
        }
        return parts[parts.length - 1];
    }

    public void write(ZooKeeperClient zkc)
        throws IOException, KeeperException.NodeExistsException {
        String finalisedData = getFinalisedData(version);
        try {
            zkc.get().create(zkPath, finalisedData.getBytes(UTF_8),
                zkc.getDefaultACL(), CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            throw nee;
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on creating ledger znode " + zkPath, ie);
        } catch (Exception e) {
            LOG.error("Error creating ledger znode {}", zkPath, e);
            throw new IOException("Error creating ledger znode " + zkPath);
        }
    }

    boolean checkEquivalence(ZooKeeperClient zkc, String path) {
        try {
            LogSegmentMetadata other = FutureUtils.result(read(zkc, path));
            if (LOG.isTraceEnabled()) {
                LOG.trace("Verifying {} against {}", this, other);
            }

            boolean retVal;

            // All fields may not be comparable so only compare the ones
            // that can be compared
            // completionTime is set when a node is finalized, so that
            // cannot be compared
            // if the node is inprogress, don't compare the lastTxId either
            if (this.getLogSegmentSequenceNumber() != other.getLogSegmentSequenceNumber() ||
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
        if (!(o instanceof LogSegmentMetadata)) {
            return false;
        }
        LogSegmentMetadata ol = (LogSegmentMetadata) o;
        return getLogSegmentSequenceNumber() == ol.getLogSegmentSequenceNumber()
            && ledgerId == ol.ledgerId
            && firstTxId == ol.firstTxId
            && lastTxId == ol.lastTxId
            && version == ol.version
            && completionTime == ol.completionTime
            && Objects.equal(lastDLSN, ol.lastDLSN)
            && Objects.equal(minActiveDLSN, ol.minActiveDLSN)
            && startSequenceId == ol.startSequenceId
            && status == ol.status;
    }

    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + (int) ledgerId;
        hash = hash * 31 + (int) firstTxId;
        hash = hash * 31 + (int) lastTxId;
        hash = hash * 31 + version.value;
        hash = hash * 31 + (int) completionTime;
        hash = hash * 31 + (int) getLogSegmentSequenceNumber();
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
            ", logSegmentSequenceNumber:" + getLogSegmentSequenceNumber() +
            ", lastEntryId:" + getLastEntryId() +
            ", lastSlotId:" + getLastSlotId() +
            ", inprogress:" + inprogress +
            ", minActiveDLSN:" + minActiveDLSN +
            ", startSequenceId:" + startSequenceId +
            "]";
    }

    public Mutator mutator() {
        return new Mutator(this);
    }


    //
    // Version Checking Utilities
    //

    public boolean supportsLogSegmentSequenceNo() {
        return supportsLogSegmentSequenceNo(version.value);
    }

    /**
     * Whether the provided version supports log segment sequence number.
     *
     * @param version
     *          log segment metadata version
     * @return true if this log segment supports log segment sequence number.
     */
    public static boolean supportsLogSegmentSequenceNo(int version) {
        return version >= LogSegmentMetadataVersion.VERSION_V2_LEDGER_SEQNO.value;
    }

    /**
     * Whether the provided version supports enveloping entries before writing to bookkeeper.
     *
     * @param version
     *          log segment metadata version
     * @return true if this log segment supports enveloping entries
     */
    public static boolean supportsEnvelopedEntries(int version) {
        return version >= LogSegmentMetadataVersion.VERSION_V4_ENVELOPED_ENTRIES.value;
    }

    public boolean supportsSequenceId() {
        return supportsSequenceId(version.value);
    }

    /**
     * Whether the provided version supports sequence id.
     *
     * @param version
     *          log segment metadata version
     * @return true if the log segment support sequence id.
     */
    public static boolean supportsSequenceId(int version) {
        return version >= LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value;
    }

}
