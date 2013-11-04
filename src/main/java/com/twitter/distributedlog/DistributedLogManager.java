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


/**
 * A DistributedLogManager is responsible for managing a single place of storing
 * edit logs. It may correspond to multiple files, a backup node, etc.
 * Even when the actual underlying storage is rolled, or failed and restored,
 * each conceptual place of storage corresponds to exactly one instance of
 * this class, which is created when the EditLog is first opened.
 */
public interface DistributedLogManager extends MetadataAccessor {

    /**
     * Begin writing to multiple partitions of the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    public PartitionAwareLogWriter startLogSegment() throws IOException;

    /**
     * Begin writing to the log stream with an async writer
     *
     * @return async writer to write log records.
     * @throws IOException
     */
    public AsyncLogWriter asyncStartLogSegmentNonPartitioned() throws IOException;

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    public LogWriter startLogSegmentNonPartitioned() throws IOException;

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException;

    /**
     * Get a reader to read a log stream as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamReader getAppendOnlyStreamReader() throws IOException;

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param partition – the partition within the log stream to read from
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    public LogReader getInputStream(PartitionId partition, long fromTxnId)
        throws IOException;

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    public LogReader getInputStream(long fromTxnId)
        throws IOException;

    /**
     * Get the last log record before the specified transactionId
     *
     * @param partition – the partition within the log stream to read from
     * @param fromTxnId - the first transaction id we want to read
     * @return the last log record before a given transactionId
     * @throws IOException if a stream cannot be found.
     */
    public long getTxIdNotLaterThan(PartitionId partition, long fromTxnId)
        throws IOException;

    public long getTxIdNotLaterThan(long fromTxnId)
        throws IOException;


    public long getFirstTxId(PartitionId partition) throws IOException;

    public long getFirstTxId() throws IOException;

    public long getLastTxId(PartitionId partition) throws IOException;

    public long getLastTxId() throws IOException;

    public void recover(PartitionId partition) throws IOException;

    public void recover() throws IOException;

    /**
     * Delete all the partitions of the specified log
     *
     * @throws IOException if the deletion fails
     */
    public void delete() throws IOException;

    /**
     * Delete the specified partition
     *
     * @param partition – the partition within the log stream to delete
     * @throws IOException if the deletion fails
     */
    public void deletePartition(PartitionId partition) throws IOException;

    /**
     * The DistributedLogManager may archive/purge any logs for transactionId
     * less than or equal to minImageTxId.
     * This is to be used only when the client explicitly manages deletion. If
     * the cleanup policy is based on sliding time window, then this method need
     * not be called.
     *
     * @param minTxIdToKeep the earliest txid that must be retained
     * @throws IOException if purging fails
     */
    public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException;
}
