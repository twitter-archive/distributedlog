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

import org.apache.bookkeeper.stats.StatsLogger;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Record reader to read records from an enveloped entry buffer.
 */
class EnvelopedEntryReader implements Entry.Reader, RecordStream {

    private final long logSegmentSeqNo;
    private final long entryId;
    private final LogRecord.Reader reader;

    // slot id
    private long slotId = 0;

    EnvelopedEntryReader(long logSegmentSeqNo,
                         long entryId,
                         long startSequenceId,
                         InputStream in,
                         boolean envelopedEntry,
                         boolean deserializeRecordSet,
                         StatsLogger statsLogger)
            throws IOException {
        this.logSegmentSeqNo = logSegmentSeqNo;
        this.entryId = entryId;
        InputStream src = in;
        if (envelopedEntry) {
            src = EnvelopedEntry.fromInputStream(in, statsLogger);
        }
        this.reader = new LogRecord.Reader(
                this,
                new DataInputStream(src),
                startSequenceId,
                deserializeRecordSet);
    }

    @Override
    public LogRecordWithDLSN nextRecord() throws IOException {
        return reader.readOp();
    }

    @Override
    public boolean skipTo(long txId) throws IOException {
        return reader.skipTo(txId, true);
    }

    @Override
    public boolean skipTo(DLSN dlsn) throws IOException {
        return reader.skipTo(dlsn);
    }

    //
    // Record Stream
    //

    @Override
    public void advance(int numRecords) {
        slotId += numRecords;
    }

    @Override
    public DLSN getCurrentPosition() {
        return new DLSN(logSegmentSeqNo, entryId, slotId);
    }

    @Override
    public String getName() {
        return "EnvelopedReader";
    }
}
