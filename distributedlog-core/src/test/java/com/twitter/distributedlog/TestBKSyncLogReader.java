package com.twitter.distributedlog;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test Sync Log Reader
 */
public class TestBKSyncLogReader extends TestDistributedLogBase {

    static final Logger logger = LoggerFactory.getLogger(TestBKSyncLogReader.class);

    @Rule
    public TestName testName = new TestName();

    @Test(timeout = 60000)
    public void testCreateReaderBeyondLastTransactionId() throws Exception {
        String name = testName.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i < 10; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        LogReader reader = dlm.getInputStream(20L);
        assertNull(reader.readNext(false));

        // write another 20 records
        out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 10; i < 30; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        for (int i = 0; i < 10; i++) {
            LogRecord record = waitForNextRecord(reader);
            assertEquals(20L + i, record.getTransactionId());
        }
        assertNull(reader.readNext(false));
    }

    @Test(timeout = 60000)
    public void testDeletingLogWhileReading() throws Exception {
        String name = testName.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i < 10; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        LogReader reader = dlm.getInputStream(1L);
        for (int i = 1; i < 10; i++) {
            LogRecord record = waitForNextRecord(reader);
            assertEquals((long) i, record.getTransactionId());
        }

        DistributedLogManager deleteDLM = createNewDLM(conf, name);
        deleteDLM.delete();

        LogRecord record;
        try {
            record = reader.readNext(false);
            while (null == record) {
                record = reader.readNext(false);
            }
            fail("Should fail reading next with LogNotFound");
        } catch (LogNotFoundException lnfe) {
            // expected
        }
    }
}
