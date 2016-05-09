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

import com.twitter.distributedlog.Entry.Writer;
import com.twitter.distributedlog.exceptions.InvalidEnvelopedEntryException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.twitter.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;

/**
 * {@link com.twitter.distributedlog.io.Buffer} based log record set writer.
 */
class EnvelopedEntryWriter implements Writer {

    static final Logger logger = LoggerFactory.getLogger(EnvelopedEntryWriter.class);

    private static class WriteRequest {

        private final int numRecords;
        private final Promise<DLSN> promise;

        WriteRequest(int numRecords, Promise<DLSN> promise) {
            this.numRecords = numRecords;
            this.promise = promise;
        }

    }

    private final String logName;
    private final Buffer buffer;
    private final LogRecord.Writer writer;
    private final List<WriteRequest> writeRequests;
    private final boolean envelopeBeforeTransmit;
    private final CompressionCodec.Type codec;
    private final StatsLogger statsLogger;
    private int count = 0;
    private boolean hasUserData = false;
    private long maxTxId = Long.MIN_VALUE;

    EnvelopedEntryWriter(String logName,
                         int initialBufferSize,
                         boolean envelopeBeforeTransmit,
                         CompressionCodec.Type codec,
                         StatsLogger statsLogger) {
        this.logName = logName;
        this.buffer = new Buffer(initialBufferSize * 6 / 5);
        this.writer = new LogRecord.Writer(new DataOutputStream(buffer));
        this.writeRequests = new LinkedList<WriteRequest>();
        this.envelopeBeforeTransmit = envelopeBeforeTransmit;
        this.codec = codec;
        this.statsLogger = statsLogger;
    }

    @Override
    public synchronized void reset() {
        cancelPromises(new WriteCancelledException(logName, "Record Set is reset"));
        count = 0;
        this.buffer.reset();
    }

    @Override
    public synchronized void writeRecord(LogRecord record,
                                         Promise<DLSN> transmitPromise)
            throws LogRecordTooLongException, WriteException {
        int logRecordSize = record.getPersistentSize();
        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(
                    "Log Record of size " + logRecordSize + " written when only "
                            + MAX_LOGRECORD_SIZE + " is allowed");
        }

        try {
            this.writer.writeOp(record);
            int numRecords = 1;
            if (!record.isControl()) {
                hasUserData = true;
            }
            if (record.isRecordSet()) {
                numRecords = LogRecordSet.numRecords(record);
            }
            count += numRecords;
            writeRequests.add(new WriteRequest(numRecords, transmitPromise));
            maxTxId = Math.max(maxTxId, record.getTransactionId());
        } catch (IOException e) {
            logger.error("Failed to append record to record set of {} : ",
                    logName, e);
            throw new WriteException(logName, "Failed to append record to record set of "
                    + logName);
        }
    }

    private synchronized void satisfyPromises(long lssn, long entryId) {
        long nextSlotId = 0;
        for (WriteRequest request : writeRequests) {
            request.promise.setValue(new DLSN(lssn, entryId, nextSlotId));
            nextSlotId += request.numRecords;
        }
        writeRequests.clear();
    }

    private synchronized void cancelPromises(Throwable reason) {
        for (WriteRequest request : writeRequests) {
            request.promise.setException(reason);
        }
        writeRequests.clear();
    }

    @Override
    public synchronized long getMaxTxId() {
        return maxTxId;
    }

    @Override
    public synchronized boolean hasUserRecords() {
        return hasUserData;
    }

    @Override
    public int getNumBytes() {
        return buffer.size();
    }

    @Override
    public synchronized int getNumRecords() {
        return count;
    }

    @Override
    public synchronized Buffer getBuffer() throws InvalidEnvelopedEntryException, IOException {
        if (!envelopeBeforeTransmit) {
            return buffer;
        }
        // We can't escape this allocation because things need to be read from one byte array
        // and then written to another. This is the destination.
        Buffer toSend = new Buffer(buffer.size());
        byte[] decompressed = buffer.getData();
        int length = buffer.size();
        EnvelopedEntry entry = new EnvelopedEntry(EnvelopedEntry.CURRENT_VERSION,
                                                  codec,
                                                  decompressed,
                                                  length,
                                                  statsLogger);
        // This will cause an allocation of a byte[] for compression. This can be avoided
        // but we can do that later only if needed.
        entry.writeFully(new DataOutputStream(toSend));
        return toSend;
    }

    @Override
    public DLSN finalizeTransmit(long lssn, long entryId) {
        return new DLSN(lssn, entryId, count - 1);
    }

    @Override
    public void completeTransmit(long lssn, long entryId) {
        satisfyPromises(lssn, entryId);
    }

    @Override
    public void abortTransmit(Throwable reason) {
        cancelPromises(reason);
    }
}
