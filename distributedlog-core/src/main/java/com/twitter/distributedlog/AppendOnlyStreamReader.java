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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendOnlyStreamReader extends InputStream {
    static final Logger LOG = LoggerFactory.getLogger(AppendOnlyStreamReader.class);

    private LogRecordWithInputStream currentLogRecord = null;
    private final DistributedLogManager dlm;
    private LogReader reader;
    private long currentPosition;
    private static final int SKIP_BUFFER_SIZE = 512;

    // Cache the input stream for a log record.
    private static class LogRecordWithInputStream {
        private final InputStream payloadStream;
        private final LogRecordWithDLSN logRecord;

        LogRecordWithInputStream(LogRecordWithDLSN logRecord) {
            Preconditions.checkNotNull(logRecord);

            LOG.debug("Got record dlsn = {}, txid = {}, len = {}",
                new Object[] {logRecord.getDlsn(), logRecord.getTransactionId(), logRecord.getPayload().length});

            this.logRecord = logRecord;
            this.payloadStream = logRecord.getPayLoadInputStream();
        }

        InputStream getPayLoadInputStream() {
            return payloadStream;
        }

        LogRecordWithDLSN getLogRecord() {
            return logRecord;
        }

        // The last txid of the log record is the position of the next byte in the stream.
        // Subtract length to get starting offset.
        long getOffset() {
            return logRecord.getTransactionId() - logRecord.getPayload().length;
        }
    }

    /**
     * Construct ledger input stream
     *
     * @param dlm the Distributed Log Manager to access the stream
     */
    AppendOnlyStreamReader(DistributedLogManager dlm)
        throws IOException {
        this.dlm = dlm;
        reader = dlm.getInputStream(0);
        currentPosition = 0;
    }

    /**
     * Get input stream representing next entry in the
     * ledger.
     *
     * @return input stream, or null if no more entries
     */
    private LogRecordWithInputStream nextLogRecord() throws IOException {
        return nextLogRecord(reader);
    }

    private static LogRecordWithInputStream nextLogRecord(LogReader reader) throws IOException {
        LogRecordWithDLSN record = reader.readNext(false);

        if (null != record) {
            return new LogRecordWithInputStream(record);
        } else {
            record = reader.readNext(false);
            if (null != record) {
                return new LogRecordWithInputStream(record);
            } else {
                LOG.debug("No record");
                return null;
            }
        }
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        if (read(b, 0, 1) != 1) {
            return -1;
        } else {
            return b[0];
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = 0;
        if (currentLogRecord == null) {
            currentLogRecord = nextLogRecord();
            if (currentLogRecord == null) {
                return read;
            }
        }

        while (read < len) {
            int thisread = currentLogRecord.getPayLoadInputStream().read(b, off + read, (len - read));
            if (thisread == -1) {
                currentLogRecord = nextLogRecord();
                if (currentLogRecord == null) {
                    return read;
                }
            } else {
                LOG.debug("Offset saved = {}, persisted = {}",
                    currentPosition, currentLogRecord.getLogRecord().getTransactionId());
                currentPosition += thisread;
                read += thisread;
            }
        }
        return read;
    }

    /**
     * Position the reader at the given offset. If we fail to skip to the desired position
     * and don't hit end of stream, return false.
     *
     * @throws com.twitter.distributedlog.exceptions.EndOfStreamException if we attempt to
     *         skip past the end of the stream.
     */
    public boolean skipTo(long position) throws IOException {

        // No need to skip anywhere.
        if (position == position()) {
            return true;
        }

        LogReader skipReader = dlm.getInputStream(position);
        LogRecordWithInputStream logRecord = null;
        try {
            logRecord = nextLogRecord(skipReader);
        } catch (IOException ex) {
            skipReader.close();
            throw ex;
        }

        if (null == logRecord) {
            return false;
        }

        // We may end up with a reader positioned *before* the requested position if
        // we're near the tail and the writer is still active, or if the desired position
        // is not at a log record payload boundary.
        // Transaction ID gives us the starting position of the log record. Read ahead
        // if necessary.
        currentPosition = logRecord.getOffset();
        currentLogRecord = logRecord;
        LogReader oldReader = reader;
        reader = skipReader;

        // Close the oldreader after swapping AppendOnlyStreamReader state. Close may fail
        // and we need to make sure it leaves AppendOnlyStreamReader in a consistent state.
        oldReader.close();

        byte[] skipBuffer = new byte[SKIP_BUFFER_SIZE];
        while (currentPosition < position) {
            long bytesToRead = Math.min(position - currentPosition, SKIP_BUFFER_SIZE);
            long bytesRead = read(skipBuffer, 0, (int)bytesToRead);
            if (bytesRead < bytesToRead) {
                return false;
            }
        }

        return true;
    }

    public long position() {
        return currentPosition;
    }
}
