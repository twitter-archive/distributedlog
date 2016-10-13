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
package com.twitter.distributedlog.mapreduce;

import com.twitter.distributedlog.*;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Enumeration;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Record Reader to read from a log segment split
 */
class LogSegmentReader extends RecordReader<DLSN, LogRecordWithDLSN> {

    final String streamName;
    final BookKeeper bk;
    final LedgerHandle lh;
    final LogSegmentMetadata metadata;

    long entryId = -1L;
    Entry.Reader reader = null;
    LogRecordWithDLSN currentRecord = null;
    int readPos = 0;

    LogSegmentReader(String streamName,
                     DistributedLogConfiguration conf,
                     BookKeeper bk,
                     LogSegmentSplit split)
            throws IOException {
        this.streamName = streamName;
        this.bk = bk;
        this.metadata = split.getMetadata();
        try {
            this.lh = bk.openLedgerNoRecovery(
                    split.getLedgerId(),
                    BookKeeper.DigestType.CRC32,
                    conf.getBKDigestPW().getBytes(UTF_8));
        } catch (BKException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // do nothing
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        LogRecordWithDLSN record;
        currentRecord = null;
        if (null != reader) {
            record = reader.nextRecord();
            if (null != record) {
                currentRecord = record;
                readPos = record.getPositionWithinLogSegment();
                return true;
            } else {
                return false;
            }
        }
        ++entryId;
        if (entryId > lh.getLastAddConfirmed()) {
            return false;
        }
        try {
            Enumeration<LedgerEntry> entries =
                    lh.readEntries(entryId, entryId);
            if (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                reader = Entry.newBuilder()
                        .setLogSegmentInfo(metadata.getLogSegmentSequenceNumber(),
                                metadata.getStartSequenceId())
                        .setEntryId(entry.getEntryId())
                        .setEnvelopeEntry(
                                LogSegmentMetadata.supportsEnvelopedEntries(metadata.getVersion()))
                        .deserializeRecordSet(true)
                        .setInputStream(entry.getEntryInputStream())
                        .buildReader();
            }
            return nextKeyValue();
        } catch (BKException e) {
            throw new IOException(e);
        }
    }

    @Override
    public DLSN getCurrentKey()
            throws IOException, InterruptedException {
        return currentRecord.getDlsn();
    }

    @Override
    public LogRecordWithDLSN getCurrentValue()
            throws IOException, InterruptedException {
        return currentRecord;
    }

    @Override
    public float getProgress()
            throws IOException, InterruptedException {
        if (metadata.getRecordCount() > 0) {
            return ((float) (readPos + 1)) / metadata.getRecordCount();
        }
        return 1;
    }

    @Override
    public void close() throws IOException {
        try {
            lh.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (BKException e) {
            throw new IOException(e);
        }
    }
}
