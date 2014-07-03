package com.twitter.distributedlog;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestTruncate extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestTruncate.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setOutputBufferSize(0).setPeriodicFlushFrequencyMilliSeconds(10);

    @Test
    public void testTruncation() throws Exception {
        String name = "distrlog-truncation";

        long txid = 1;
        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        for (long i = 1; i <= 4; i++) {
            LOG.info("Writing Log Segment {}.", i);
            DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
            AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 1; j <= 10; j++) {
                long curTxId = txid++;
                DLSN dlsn = writer.write(DLMTestUtil.getLogRecordInstance(curTxId)).get();
                txid2DLSN.put(curTxId, dlsn);
            }
            writer.close();
            dlm.close();
        }

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        for (int j = 1; j <= 10; j++) {
            long curTxId = txid++;
            DLSN dlsn = writer.write(DLMTestUtil.getLogRecordInstance(curTxId)).get();
            txid2DLSN.put(curTxId, dlsn);
        }

        Thread.sleep(1000);

        // delete invalid dlsn
        assertFalse(writer.truncate(DLSN.InvalidDLSN).get());
        verifyEntries(name, 1, 1, 5 * 10);

        for (int i = 1; i <= 4; i++) {
            int txn = (i-1) * 10 + i;
            DLSN dlsn = txid2DLSN.get((long)txn);
            assertTrue(writer.truncate(dlsn).get());
            verifyEntries(name, 1, (i - 1) * 10 + 1, (5 - i + 1) * 10);
        }

        // Delete higher dlsn
        int txn = 43;
        DLSN dlsn = txid2DLSN.get((long) txn);
        assertTrue(writer.truncate(dlsn).get());
        verifyEntries(name, 1, 41, 10);

        writer.close();
        dlm.close();
    }

    private void verifyEntries(String name, long readFromTxId, long startTxId, int numEntries) throws Exception {
        DistributedLogManager dlm = DLMTestUtil.createNewDLM(conf, name);
        LogReader reader = dlm.getInputStream(readFromTxId);

        long txid = startTxId;
        int numRead = 0;
        LogRecord r = reader.readNext(false);
        while (null != r) {
            DLMTestUtil.verifyLogRecord(r);
            LOG.trace("Read entry {}.", r.getTransactionId());
            assertEquals(txid++, r.getTransactionId());
            ++numRead;
            r = reader.readNext(false);
        }
        assertEquals(numEntries, numRead);
        reader.close();
        dlm.close();
    }

}
