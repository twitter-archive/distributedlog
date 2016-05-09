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
package com.twitter.distributedlog.impl;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * Ledger based log segment entry writer.
 */
public class BKLogSegmentEntryWriter implements LogSegmentEntryWriter {

    private final LedgerHandle lh;

    public BKLogSegmentEntryWriter(LedgerHandle lh) {
        this.lh = lh;
    }

    @VisibleForTesting
    public LedgerHandle getLedgerHandle() {
        return this.lh;
    }

    @Override
    public long getLogSegmentId() {
        return lh.getId();
    }

    @Override
    public void asyncClose(AsyncCallback.CloseCallback callback, Object ctx) {
        lh.asyncClose(callback, ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length,
                              AsyncCallback.AddCallback callback, Object ctx) {
        lh.asyncAddEntry(data, offset, length, callback, ctx);
    }

    @Override
    public long size() {
        return lh.getLength();
    }
}
