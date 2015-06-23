package com.twitter.distributedlog.admin;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.metadata.DryrunZkMetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.TimeoutException;

import static org.junit.Assert.*;

public class TestDistributedLogAdmin extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogAdmin.class);

    private ZooKeeperClient zooKeeperClient;

    @Before
    public void setup() throws Exception {
        zooKeeperClient = ZooKeeperClientBuilder
            .newBuilder()
            .uri(createDLMURI("/"))
            .sessionTimeoutMs(10000)
            .zkAclId(null)
            .build();
        conf.setTraceReadAheadMetadataChanges(true);
    }

    @After
    public void teardown() throws Exception {
        zooKeeperClient.close();
    }


    @Test(timeout = 60000)
    public void testChangeSequenceNumber() throws Exception {
        URI uri = createDLMURI("/change-sequence-number");
        zooKeeperClient.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        String streamName = "change-sequence-number";

        // create completed log segments
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 4, 10);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, conf, 5, 41, false, 10, true);
        dlm.close();

        // create a reader
        DistributedLogManager readDLM = factory.createDistributedLogManagerWithSharedClients(streamName);
        AsyncLogReader reader = readDLM.getAsyncLogReader(DLSN.InitialDLSN);

        // read the records
        long expectedTxId = 1L;
        for (int i = 0; i < 4 * 10; i++) {
            LogRecord record = Await.result(reader.readNext());
            assertNotNull(record);
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
        }

        dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, conf, 3L, 5 * 10 + 1, true, 10, false);

        // Wait for reader to be aware of new log segments
        TimeUnit.SECONDS.sleep(2);

        DLSN dlsn = readDLM.getLastDLSN();
        assertTrue(dlsn.compareTo(new DLSN(5, Long.MIN_VALUE, Long.MIN_VALUE)) < 0);
        assertTrue(dlsn.compareTo(new DLSN(4, -1, Long.MIN_VALUE)) > 0);
        // there isn't records should be read
        Future<LogRecordWithDLSN> readFuture = reader.readNext();
        try {
            Await.result(readFuture, Duration.fromMilliseconds(1000));
            fail("Should fail reading next when there is a corrupted log segment");
        } catch (TimeoutException te) {
            // expected
        }

        // Dryrun
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(factory,
                new DryrunZkMetadataUpdater(conf, getZooKeeperClient(factory)), streamName, false, false);

        // Wait for reader to be aware of new log segments
        TimeUnit.SECONDS.sleep(2);

        dlsn = readDLM.getLastDLSN();
        assertTrue(dlsn.compareTo(new DLSN(5, Long.MIN_VALUE, Long.MIN_VALUE)) < 0);
        assertTrue(dlsn.compareTo(new DLSN(4, -1, Long.MIN_VALUE)) > 0);
        // there isn't records should be read
        try {
            Await.result(readFuture, Duration.fromMilliseconds(1000));
            fail("Should fail reading next when there is a corrupted log segment");
        } catch (TimeoutException te) {
            // expected
        }

        // Actual run
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(factory,
                ZkMetadataUpdater.createMetadataUpdater(conf, getZooKeeperClient(factory)), streamName, false, false);

        // Wait for reader to be aware of new log segments
        TimeUnit.SECONDS.sleep(2);

        expectedTxId = 51L;
        LogRecord record = Await.result(readFuture);
        assertNotNull(record);
        DLMTestUtil.verifyLogRecord(record);
        assertEquals(expectedTxId, record.getTransactionId());
        expectedTxId++;

        for (int i = 1; i < 10; i++) {
            record = Await.result(reader.readNext());
            assertNotNull(record);
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
        }

        dlsn = readDLM.getLastDLSN();
        LOG.info("LastDLSN after fix inprogress segment : {}", dlsn);
        assertTrue(dlsn.compareTo(new DLSN(7, Long.MIN_VALUE, Long.MIN_VALUE)) < 0);
        assertTrue(dlsn.compareTo(new DLSN(6, -1, Long.MIN_VALUE)) > 0);

        reader.close();
        readDLM.close();

        dlm.close();
        factory.close();
    }
}
