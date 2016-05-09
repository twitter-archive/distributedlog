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
package com.twitter.distributedlog.benchmark.stream;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;

/**
 * Read ledgers in batches
 */
public class LedgerBatchReader implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(LedgerBatchReader.class);

    private final LedgerHandle lh;
    private final ReadEntryListener readEntryListener;
    private final int batchSize;

    public LedgerBatchReader(LedgerHandle lh,
                             ReadEntryListener readEntryListener,
                             int batchSize) {
        this.lh = lh;
        this.batchSize = batchSize;
        this.readEntryListener = readEntryListener;
    }

    @Override
    public void run() {
        long lac = lh.getLastAddConfirmed();

        long entryId = 0L;

        while (entryId <= lac) {
            long startEntryId = entryId;
            long endEntryId = Math.min(startEntryId + batchSize - 1, lac);

            Enumeration<LedgerEntry> entries = null;
            while (null == entries) {
                try {
                    entries = lh.readEntries(startEntryId, endEntryId);
                } catch (BKException bke) {
                    logger.error("Encountered exceptions on reading [ {} - {} ] ",
                            new Object[] { startEntryId, endEntryId, bke });
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (null == entries) {
                break;
            }

            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                readEntryListener.onEntryComplete(BKException.Code.OK, lh, entry, null);
            }

            entryId = endEntryId + 1;
        }

    }
}
