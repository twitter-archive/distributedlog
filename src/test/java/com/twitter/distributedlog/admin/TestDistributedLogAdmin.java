package com.twitter.distributedlog.admin;

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.metadata.DryrunZkMetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestDistributedLogAdmin {

    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogAdmin.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setEnableLedgerAllocatorPool(true).setLedgerAllocatorPoolName("test");
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numBookies = 3;

    private ZooKeeperClient zkc;

    @BeforeClass
    public static void setupZooKeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void shutdownZooKeeper() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder().uri(DLMTestUtil.createDLMURI("/"))
                .sessionTimeoutMs(10000).build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testChangeSequenceNumber() throws Exception {
        URI uri = DLMTestUtil.createDLMURI("/dlog_admin");
        zkc.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        String streamName = "change-sequence-number";

        // create completed log segments
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 4, 10);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, conf, 5, 41, false, 10, true);
        dlm.close();

        // create a reader
        DistributedLogManager readDLM = factory.createDistributedLogManagerWithSharedClients(streamName);
        LogReader reader = readDLM.getInputStream(0L);

        // read the records
        long numTrans = 0L;
        LogRecord record = reader.readNext(false);
        long expectedTxId = 1L;
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals(4 * 10, numTrans);

        dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        DLMTestUtil.injectLogSegmentWithGivenLedgerSeqNo(dlm, conf, 3L, 5 * 10 + 1, true, 10, false);

        // Wait for reader to be aware of new log segments
        TimeUnit.SECONDS.sleep(2);

        DLSN dlsn = readDLM.getLastDLSN();
        assertTrue(dlsn.compareTo(new DLSN(5, Long.MIN_VALUE, Long.MIN_VALUE)) < 0);
        assertTrue(dlsn.compareTo(new DLSN(4, -1, Long.MIN_VALUE)) > 0);
        // there isn't records should be read
        assertNull(reader.readNext(false));

        // Dryrun
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(factory,
                new DryrunZkMetadataUpdater(factory.getSharedWriterZKCForDL()), streamName, false, false);

        // Wait for reader to be aware of new log segments
        TimeUnit.SECONDS.sleep(2);

        dlsn = readDLM.getLastDLSN();
        assertTrue(dlsn.compareTo(new DLSN(5, Long.MIN_VALUE, Long.MIN_VALUE)) < 0);
        assertTrue(dlsn.compareTo(new DLSN(4, -1, Long.MIN_VALUE)) > 0);
        // there isn't records should be read
        assertNull(reader.readNext(false));

        // Actual run
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(factory,
                ZkMetadataUpdater.createMetadataUpdater(factory.getSharedWriterZKCForDL()), streamName, false, false);

        // Wait for reader to be aware of new log segments
        TimeUnit.SECONDS.sleep(2);

        record = reader.readNext(false);
        expectedTxId = 51L;
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            numTrans++;
            expectedTxId++;
            record = reader.readNext(false);
        }

        dlsn = readDLM.getLastDLSN();
        LOG.info("LastDLSN after fix inprogress segment : {}", dlsn);
        assertTrue(dlsn.compareTo(new DLSN(7, Long.MIN_VALUE, Long.MIN_VALUE)) < 0);
        assertTrue(dlsn.compareTo(new DLSN(6, -1, Long.MIN_VALUE)) > 0);
        assertEquals(5 * 10, numTrans);

        reader.close();
        readDLM.close();

        dlm.close();
        factory.close();
    }
}
