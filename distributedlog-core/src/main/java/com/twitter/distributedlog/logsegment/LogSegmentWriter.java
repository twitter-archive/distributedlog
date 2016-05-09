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
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.exceptions.BKTransmitException;
import com.twitter.distributedlog.exceptions.LockingException;
import com.twitter.distributedlog.io.AsyncAbortable;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.util.Future;

import java.io.IOException;

/**
 * An interface class to write log records into a log segment.
 */
@Beta
public interface LogSegmentWriter extends AsyncCloseable, AsyncAbortable {

    /**
     * Get the unique log segment id.
     *
     * @return log segment id.
     */
    public long getLogSegmentId();

    /**
     * Write a log record to a log segment.
     *
     * @param record single log record
     * @return a future representing write result. A {@link DLSN} is returned if write succeeds,
     *         otherwise, exceptions are returned.
     * @throws com.twitter.distributedlog.exceptions.LogRecordTooLongException if log record is too long
     * @throws com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException on invalid enveloped entry
     * @throws LockingException if failed to acquire lock for the writer
     * @throws BKTransmitException if failed to transmit data to bk
     * @throws com.twitter.distributedlog.exceptions.WriteException if failed to write to bk
     */
    public Future<DLSN> asyncWrite(LogRecord record);

    /**
     * This isn't a simple synchronous version of {@code asyncWrite}. It has different semantic.
     * This method only writes data to the buffer and flushes buffer if needed.
     *
     * TODO: we should remove this method. when we rewrite synchronous writer based on asynchronous writer,
     *       since this is the semantic needed to be provided in higher level but just calling write & flush.
     *
     * @param record single log record
     * @throws IOException when tried to flush the buffer.
     * @see {@link LogSegmentWriter#asyncWrite}
     */
    public void write(LogRecord record) throws IOException;

    /**
     * Transmit the buffered data and wait for it being persisted and return the last acknowledged
     * transaction id.
     *
     * @return future representing the transmit result with last acknowledged transaction id.
     */
    public Future<Long> flush();

    /**
     * Commit the current acknowledged data. It is the consequent operation of {@link #flush()},
     * which makes all the acknowledged data visible to
     *
     * @return
     */
    public Future<Long> commit();

}
