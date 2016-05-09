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
package com.twitter.distributedlog.metadata;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

public class LogSegmentMetadataStoreUpdater implements MetadataUpdater {

    static final Logger LOG = LoggerFactory.getLogger(LogSegmentMetadataStoreUpdater.class);

    public static MetadataUpdater createMetadataUpdater(DistributedLogConfiguration conf,
                                                        LogSegmentMetadataStore metadataStore) {
        return new LogSegmentMetadataStoreUpdater(conf, metadataStore);
    }

    protected final LogSegmentMetadataStore metadataStore;
    protected final LogSegmentMetadata.LogSegmentMetadataVersion metadataVersion;

    protected LogSegmentMetadataStoreUpdater(DistributedLogConfiguration conf,
                                             LogSegmentMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.metadataVersion = LogSegmentMetadata.LogSegmentMetadataVersion.of(conf.getDLLedgerMetadataLayoutVersion());
    }

    private String formatLogSegmentSequenceNumber(long logSegmentSeqNo) {
        return String.format("%018d", logSegmentSeqNo);
    }

    @Override
    public Transaction<Object> transaction() {
        return metadataStore.transaction();
    }

    @Override
    public Future<LogSegmentMetadata> updateLastRecord(LogSegmentMetadata segment,
                                                       LogRecordWithDLSN record) {
        DLSN dlsn = record.getDlsn();
        Preconditions.checkState(!segment.isInProgress(),
                "Updating last dlsn for an inprogress log segment isn't supported.");
        Preconditions.checkArgument(segment.isDLSNinThisSegment(dlsn),
                "DLSN " + dlsn + " doesn't belong to segment " + segment);
        final LogSegmentMetadata newSegment = segment.mutator()
                .setLastDLSN(dlsn)
                .setLastTxId(record.getTransactionId())
                .setRecordCount(record)
                .build();
        return updateSegmentMetadata(newSegment);
    }

    @Override
    public Future<LogSegmentMetadata> changeSequenceNumber(LogSegmentMetadata segment,
                                                           long logSegmentSeqNo) {
        String newZkPath = segment.getZkPath()
                .replace(formatLogSegmentSequenceNumber(segment.getLogSegmentSequenceNumber()),
                        formatLogSegmentSequenceNumber(logSegmentSeqNo));
        final LogSegmentMetadata newSegment = segment.mutator()
                .setLogSegmentSequenceNumber(logSegmentSeqNo)
                .setZkPath(newZkPath)
                .build();
        return addNewSegmentAndDeleteOldSegment(newSegment, segment);
    }

    /**
     * Change the truncation status of a <i>log segment</i> to be active
     *
     * @param segment log segment to change truncation status to active.
     * @return new log segment
     */
    @Override
    public Future<LogSegmentMetadata> setLogSegmentActive(LogSegmentMetadata segment) {
        final LogSegmentMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentMetadata.TruncationStatus.ACTIVE)
            .build();
        return addNewSegmentAndDeleteOldSegment(newSegment, segment);
    }

    /**
     * Change the truncation status of a <i>log segment</i> to truncated
     *
     * @param segment log segment to change truncation status to truncated.
     * @return new log segment
     */
    @Override
    public Future<LogSegmentMetadata> setLogSegmentTruncated(LogSegmentMetadata segment) {
        final LogSegmentMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentMetadata.TruncationStatus.TRUNCATED)
            .build();
        return addNewSegmentAndDeleteOldSegment(newSegment, segment);
    }

    @Override
    public LogSegmentMetadata setLogSegmentTruncated(Transaction<Object> txn, LogSegmentMetadata segment) {
        final LogSegmentMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentMetadata.TruncationStatus.TRUNCATED)
            .build();
        addNewSegmentAndDeleteOldSegment(txn, newSegment, segment);
        return newSegment;
    }

    /**
     * Change the truncation status of a <i>log segment</i> to partially truncated
     *
     * @param segment log segment to change sequence number.
     * @param minActiveDLSN DLSN within the log segment before which log has been truncated
     * @return new log segment
     */
    @Override
    public Future<LogSegmentMetadata> setLogSegmentPartiallyTruncated(LogSegmentMetadata segment, DLSN minActiveDLSN) {
        final LogSegmentMetadata newSegment = segment.mutator()
            .setTruncationStatus(LogSegmentMetadata.TruncationStatus.PARTIALLY_TRUNCATED)
            .setMinActiveDLSN(minActiveDLSN)
            .build();
        return addNewSegmentAndDeleteOldSegment(newSegment, segment);
    }

    @Override
    public LogSegmentMetadata setLogSegmentPartiallyTruncated(Transaction<Object> txn,
                                                              LogSegmentMetadata segment,
                                                              DLSN minActiveDLSN) {
        final LogSegmentMetadata newSegment = segment.mutator()
                .setTruncationStatus(LogSegmentMetadata.TruncationStatus.PARTIALLY_TRUNCATED)
                .setMinActiveDLSN(minActiveDLSN)
                .build();
        addNewSegmentAndDeleteOldSegment(txn, newSegment, segment);
        return newSegment;
    }

    protected Future<LogSegmentMetadata> updateSegmentMetadata(final LogSegmentMetadata segment) {
        Transaction<Object> txn = transaction();
        metadataStore.updateLogSegment(txn, segment);
        return txn.execute().map(new AbstractFunction1<Void, LogSegmentMetadata>() {
            @Override
            public LogSegmentMetadata apply(Void value) {
                return segment;
            }
        });
    }

    protected Future<LogSegmentMetadata> addNewSegmentAndDeleteOldSegment(
            final LogSegmentMetadata newSegment, LogSegmentMetadata oldSegment) {
        LOG.info("old segment {} new segment {}", oldSegment, newSegment);
        Transaction<Object> txn = transaction();
        addNewSegmentAndDeleteOldSegment(txn, newSegment, oldSegment);
        return txn.execute().map(new AbstractFunction1<Void, LogSegmentMetadata>() {
            @Override
            public LogSegmentMetadata apply(Void value) {
                return newSegment;
            }
        });
    }

    protected void addNewSegmentAndDeleteOldSegment(Transaction<Object> txn,
                                                    LogSegmentMetadata newSegment,
                                                    LogSegmentMetadata oldSegment) {
        metadataStore.deleteLogSegment(txn, oldSegment);
        metadataStore.createLogSegment(txn, newSegment);
    }

}
