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
package com.twitter.distributedlog.logsegment;

import com.google.common.annotations.Beta;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.callback.LogSegmentNamesListener;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.distributedlog.util.Transaction.OpListener;
import com.twitter.util.Future;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

import java.io.Closeable;
import java.util.List;

/**
 * Interface for log segment metadata store. All operations that modify log segments should
 * be executed under a {@link Transaction}.
 */
@Beta
public interface LogSegmentMetadataStore extends Closeable {

    /**
     * Start the transaction on changing log segment metadata store.
     *
     * @return transaction of the log segment metadata store.
     */
    Transaction<Object> transaction();

    // The reason to keep storing log segment sequence number & log record transaction id
    // in this log segment metadata store interface is to share the transaction that used
    // to start/complete log segment. It is a bit hard to separate them out right now.

    /**
     * Store the maximum log segment sequence number on <code>path</code>.
     *
     * @param txn
     *          transaction to execute for storing log segment sequence number.
     * @param path
     *          path to store sequence number
     * @param sequenceNumber
     *          log segment sequence number to store
     * @param listener
     *          listener on the result to this operation
     */
    void storeMaxLogSegmentSequenceNumber(Transaction<Object> txn,
                                          String path,
                                          Versioned<Long> sequenceNumber,
                                          OpListener<Version> listener);

    /**
     * Store the maximum transaction id for <code>path</code>
     *
     * @param txn
     *          transaction to execute for storing transaction id
     * @param path
     *          path to store sequence number
     * @param transactionId
     *          transaction id to store
     * @param listener
     *          listener on the result to this operation
     */
    void storeMaxTxnId(Transaction<Object> txn,
                       String path,
                       Versioned<Long> transactionId,
                       OpListener<Version> listener);

    /**
     * Create a log segment <code>segment</code> under transaction <code>txn</code>.
     *
     * NOTE: this operation shouldn't be a blocking call. and it shouldn't execute the operation
     *       immediately. the operation should be executed via {@link Transaction#execute()}
     *
     * @param txn
     *          transaction to execute for this operation
     * @param segment
     *          segment to create
     */
    void createLogSegment(Transaction<Object> txn, LogSegmentMetadata segment);

    /**
     * Delete a log segment <code>segment</code> under transaction <code>txn</code>.
     *
     * NOTE: this operation shouldn't be a blocking call. and it shouldn't execute the operation
     *       immediately. the operation should be executed via {@link Transaction#execute()}
     *
     * @param txn
     *          transaction to execute for this operation
     * @param segment
     *          segment to delete
     */
    void deleteLogSegment(Transaction<Object> txn, LogSegmentMetadata segment);

    /**
     * Update a log segment <code>segment</code> under transaction <code>txn</code>.
     *
     * NOTE: this operation shouldn't be a blocking call. and it shouldn't execute the operation
     *       immediately. the operation should be executed via {@link Transaction#execute()}
     *
     * @param txn
     *          transaction to execute for this operation
     * @param segment
     *          segment to update
     */
    void updateLogSegment(Transaction<Object> txn, LogSegmentMetadata segment);

    /**
     * Retrieve the log segment associated <code>path</code>.
     *
     * @param logSegmentPath
     *          path to store log segment metadata
     * @return future of the retrieved log segment metadata
     */
    Future<LogSegmentMetadata> getLogSegment(String logSegmentPath);

    /**
     * Retrieve the list of log segments under <code>logSegmentsPath</code>.
     *
     * @param logSegmentsPath
     *          path to store list of log segments
     * @return future of the retrieved list of log segment names
     */
    Future<List<String>> getLogSegmentNames(String logSegmentsPath);

    /**
     * Register a log segment <code>listener</code> on log segment changes under <code>logSegmentsPath</code>.
     *
     * @param logSegmentsPath
     *          log segments path
     * @param listener
     *          log segment listener on log segment changes
     */
    void registerLogSegmentListener(String logSegmentsPath,
                                    LogSegmentNamesListener listener);

    /**
     * Unregister a log segment <code>listener</code> on log segment changes under <code>logSegmentsPath</code>.
     *
     * @param logSegmentsPath
     *          log segments path
     * @param listener
     *          log segment listener on log segment changes
     */
    void unregisterLogSegmentListener(String logSegmentsPath,
                                      LogSegmentNamesListener listener);
}
