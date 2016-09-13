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

import com.twitter.distributedlog.util.FutureUtils;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test {@link LedgerHandleCache}
 */
public class TestLedgerHandleCache extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestLedgerHandleCache.class);

    protected static String ledgersPath = "/ledgers";

    private ZooKeeperClient zkc;
    private BookKeeperClient bkc;

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .zkServers(zkServers)
                .build();
        bkc = BookKeeperClientBuilder.newBuilder()
                .name("bkc")
                .zkc(zkc)
                .ledgersPath(ledgersPath)
                .dlConfig(conf)
                .build();
    }

    @After
    public void teardown() throws Exception {
        bkc.close();
        zkc.close();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testBuilderWithoutBKC() throws Exception {
        LedgerHandleCache.newBuilder().build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testBuilderWithoutStatsLogger() throws Exception {
        LedgerHandleCache.newBuilder().bkc(bkc).conf(conf).statsLogger(null).build();
    }

    @Test(timeout = 60000, expected = BKException.BKBookieHandleNotAvailableException.class)
    public void testOpenLedgerWhenBkcClosed() throws Exception {
        BookKeeperClient newBkc = BookKeeperClientBuilder.newBuilder().name("newBkc")
                .zkc(zkc).ledgersPath(ledgersPath).dlConfig(conf).build();
        LedgerHandleCache cache =
                LedgerHandleCache.newBuilder().bkc(newBkc).conf(conf).build();
        // closed the bkc
        newBkc.close();
        // open ledger after bkc closed.
        cache.openLedger(new LogSegmentMetadata.LogSegmentMetadataBuilder("", 2, 1, 1).setRegionId(1).build(), false);
    }

    @Test(timeout = 60000, expected = BKException.ZKException.class)
    public void testOpenLedgerWhenZkClosed() throws Exception {
        ZooKeeperClient newZkc = TestZooKeeperClientBuilder.newBuilder()
                .name("zkc-openledger-when-zk-closed")
                .zkServers(zkServers)
                .build();
        BookKeeperClient newBkc = BookKeeperClientBuilder.newBuilder()
                .name("bkc-openledger-when-zk-closed")
                .zkc(newZkc)
                .ledgersPath(ledgersPath)
                .dlConfig(conf)
                .build();
        try {
            LedgerHandle lh = newBkc.get().createLedger(BookKeeper.DigestType.CRC32, "zkcClosed".getBytes(UTF_8));
            lh.close();
            newZkc.close();
            LedgerHandleCache cache =
                    LedgerHandleCache.newBuilder().bkc(newBkc).conf(conf).build();
            // open ledger after zkc closed
            cache.openLedger(new LogSegmentMetadata.LogSegmentMetadataBuilder("",
                    2, lh.getId(), 1).setLogSegmentSequenceNo(lh.getId()).build(), false);
        } finally {
            newBkc.close();
        }
    }

    @Test(timeout = 60000, expected = BKException.BKUnexpectedConditionException.class)
    public void testReadLastConfirmedWithoutOpeningLedger() throws Exception {
        LedgerDescriptor desc = new LedgerDescriptor(9999, 9999, false);
        LedgerHandleCache cache =
                LedgerHandleCache.newBuilder().bkc(bkc).conf(conf).build();
        // read last confirmed
        cache.tryReadLastConfirmed(desc);
    }

    @Test(timeout = 60000, expected = BKException.BKUnexpectedConditionException.class)
    public void testReadEntriesWithoutOpeningLedger() throws Exception {
        LedgerDescriptor desc = new LedgerDescriptor(9999, 9999, false);
        LedgerHandleCache cache =
                LedgerHandleCache.newBuilder().bkc(bkc).conf(conf).build();
        // read entries
        cache.readEntries(desc, 0, 10);
    }

    @Test(timeout = 60000, expected = BKException.BKUnexpectedConditionException.class)
    public void testGetLastConfirmedWithoutOpeningLedger() throws Exception {
        LedgerDescriptor desc = new LedgerDescriptor(9999, 9999, false);
        LedgerHandleCache cache =
                LedgerHandleCache.newBuilder().bkc(bkc).conf(conf).build();
        // read entries
        cache.getLastAddConfirmed(desc);
    }

    @Test(timeout = 60000, expected = BKException.BKUnexpectedConditionException.class)
    public void testReadLastConfirmedAndEntryWithoutOpeningLedger() throws Exception {
        LedgerDescriptor desc = new LedgerDescriptor(9999, 9999, false);
        LedgerHandleCache cache =
                LedgerHandleCache.newBuilder().bkc(bkc).conf(conf).build();
        // read entries
        FutureUtils.bkResult(cache.asyncReadLastConfirmedAndEntry(desc, 1L, 200L, false));
    }

    @Test(timeout = 60000, expected = BKException.BKUnexpectedConditionException.class)
    public void testGetLengthWithoutOpeningLedger() throws Exception {
        LedgerDescriptor desc = new LedgerDescriptor(9999, 9999, false);
        LedgerHandleCache cache =
                LedgerHandleCache.newBuilder().bkc(bkc).conf(conf).build();
        // read entries
        cache.getLength(desc);
    }

    @Test(timeout = 60000)
    public void testOpenAndCloseLedger() throws Exception {
        LedgerHandle lh = bkc.get().createLedger(1, 1, 1,
                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
        LedgerHandleCache cache =
                LedgerHandleCache.newBuilder().bkc(bkc).conf(conf).build();
        LogSegmentMetadata segment = new LogSegmentMetadata.LogSegmentMetadataBuilder(
                "/data", LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID, lh.getId(), 0L)
                .build();
        LedgerDescriptor desc1 = cache.openLedger(segment, false);
        assertTrue(cache.handlesMap.containsKey(desc1));
        LedgerHandleCache.RefCountedLedgerHandle refLh = cache.handlesMap.get(desc1);
        assertEquals(1, refLh.getRefCount());
        cache.openLedger(segment, false);
        assertTrue(cache.handlesMap.containsKey(desc1));
        assertEquals(2, refLh.getRefCount());
        // close the ledger
        cache.closeLedger(desc1);
        assertTrue(cache.handlesMap.containsKey(desc1));
        assertEquals(1, refLh.getRefCount());
        cache.closeLedger(desc1);
        assertFalse(cache.handlesMap.containsKey(desc1));
        assertEquals(0, refLh.getRefCount());
    }
}
