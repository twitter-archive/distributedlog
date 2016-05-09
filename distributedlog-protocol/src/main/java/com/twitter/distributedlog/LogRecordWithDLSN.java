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

/**
 * Log record with {@link DLSN} and <code>SequenceId</code>.
 *
 * <h3>Sequence Numbers</h3>
 *
 * A log record will be assigned with an unique system generated sequence number {@link DLSN} when it is
 * written to a log. At the mean time, a 64-bits long number is assigned to the record indicates its position
 * within a log, which is called <code>SequenceId</code>. Besides {@link DLSN} and <code>SequenceID</code>,
 * application can assign its own sequence number (called <code>TransactionID</code>) to the log record while
 * writing it.
 *
 * <h4>Transaction ID</h4>
 *
 * Transaction ID is a positive 64-bits long number that is assigned by the application. It is a very helpful
 * field when application wants to organize the records and position the readers using their own sequencing method.
 * A typical use case of <code>TransactionID</code> is DistributedLog Write Proxy. It assigns the non-decreasing
 * timestamps to log records, which the timestamps could be used as `physical time` to implement `TTL` in a strong
 * consistent database to achieve consistent `TTL` over replicas.
 *
 * <h4>DLSN</h4>
 *
 * DistributedLog Sequence Number (<i>DLSN</i>) is the sequence number generated during written time. It is comparable
 * and could be used to figure out the order between records. The DLSN is comprised with 3 components. They are
 * <i>Log Segment Sequence Number</i>, <i>Entry Id</i> and <i>Slot Id</i>. (See {@link DLSN} for more details).
 * The DLSN is usually used for comparison, positioning and truncation.
 *
 * <h4>Sequence ID</h4>
 *
 * Sequence ID is introduced to address the drawback of <code>DLSN</code>, in favor of answering questions like
 * `how many records written between two DLSNs`. It is a 64-bits monotonic increasing number (starting from zero).
 * Sequence ids are only accessible by readers. That means writers don't know the sequence ids of records after they
 * wrote them.
 */
public class LogRecordWithDLSN extends LogRecord {
    private final DLSN dlsn;
    private final long startSequenceIdOfCurrentSegment;

    /**
     * This empty constructor can only be called from Reader#readOp.
     */
    LogRecordWithDLSN(DLSN dlsn, long startSequenceIdOfCurrentSegment) {
        super();
        this.dlsn = dlsn;
        this.startSequenceIdOfCurrentSegment = startSequenceIdOfCurrentSegment;
    }

    @VisibleForTesting
    public LogRecordWithDLSN(DLSN dlsn, long txid, byte[] data, long startSequenceIdOfCurrentSegment) {
        super(txid, data);
        this.dlsn = dlsn;
        this.startSequenceIdOfCurrentSegment = startSequenceIdOfCurrentSegment;
    }

    long getStartSequenceIdOfCurrentSegment() {
        return startSequenceIdOfCurrentSegment;
    }

    /**
     * Get the sequence id of the record in the log.
     *
     * @return sequence id of the record in the log.
     */
    public long getSequenceId() {
        return startSequenceIdOfCurrentSegment + getPositionWithinLogSegment() - 1;
    }

    /**
     * Get the DLSN of the record in the log.
     *
     * @return DLSN of the record in the log.
     */
    public DLSN getDlsn() {
        return dlsn;
    }

    @Override
    public String toString() {
        return "LogRecordWithDLSN{" +
            "dlsn=" + dlsn +
            ", txid=" + getTransactionId() +
            ", position=" + getPositionWithinLogSegment() +
            ", isControl=" + isControl() +
            ", isEndOfStream=" + isEndOfStream() +
            '}';
    }
}
