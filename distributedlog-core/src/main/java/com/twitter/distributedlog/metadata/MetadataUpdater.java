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

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.util.Future;

/**
 * An updater to update metadata. It contains utility functions on mutating metadata.
 */
public interface MetadataUpdater {

    /**
     * Start a transaction on metadata updates
     *
     * @return transaction
     */
    Transaction<Object> transaction();

    /**
     * Update the log segment metadata with correct last <i>record</i>.
     *
     * @param segment
     *          log segment to update last dlsn.
     * @param record
     *          correct last record.
     * @return new log segment
     */
    Future<LogSegmentMetadata> updateLastRecord(LogSegmentMetadata segment,
                                                LogRecordWithDLSN record);

    /**
     * Change ledger sequence number of <i>segment</i> to given <i>logSegmentSeqNo</i>.
     *
     * @param segment
     *          log segment to change sequence number.
     * @param logSegmentSeqNo
     *          ledger sequence number to change.
     * @return new log segment
     */
    Future<LogSegmentMetadata> changeSequenceNumber(LogSegmentMetadata segment,
                                                    long logSegmentSeqNo);

    /**
     * Change the truncation status of a <i>log segment</i> to be active
     *
     * @param segment
     *          log segment to change truncation status to active.
     * @return new log segment
     */
    Future<LogSegmentMetadata> setLogSegmentActive(LogSegmentMetadata segment);

    /**
     * Change the truncation status of a <i>log segment</i> to truncated
     *
     * @param segment
     *          log segment to change truncation status to truncated.
     * @return new log segment
     */
    Future<LogSegmentMetadata> setLogSegmentTruncated(LogSegmentMetadata segment);

    /**
     * Change the truncation status of a <i>log segment</i> to truncated. The operation won't be executed
     * immediately. The update only happens after {@link Transaction#execute()}.
     *
     * @param txn
     *          transaction used to set the log segment status
     * @param segment
     *          segment to set truncation status to truncated
     * @return log segment that truncation status is set to truncated.
     */
    LogSegmentMetadata setLogSegmentTruncated(Transaction<Object> txn, LogSegmentMetadata segment);

    /**
     * Change the truncation status of a <i>log segment</i> to partially truncated
     *
     * @param segment
     *          log segment to change sequence number.
     * @param minActiveDLSN
     *          DLSN within the log segment before which log has been truncated
     * @return new log segment
     */
    Future<LogSegmentMetadata> setLogSegmentPartiallyTruncated(LogSegmentMetadata segment,
                                                               DLSN minActiveDLSN);

    /**
     * Change the truncation status of a <i>log segment</i> to partially truncated. The operation won't be
     * executed until {@link Transaction#execute()}.
     *
     * @param txn
     *          transaction used to set the log segment status
     * @param segment
     *          segment to set truncation status to partially truncated
     * @param minActiveDLSN
     *          DLSN within the log segment before which log has been truncated
     * @return log segment that truncation status has been set to partially truncated
     */
    LogSegmentMetadata setLogSegmentPartiallyTruncated(Transaction<Object> txn,
                                                       LogSegmentMetadata segment,
                                                       DLSN minActiveDLSN);

}
