package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestDistributedLogManagerFactory {

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setEnableLedgerAllocatorPool(true).setLedgerAllocatorPoolName("test");
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numBookies = 3;

    private ZooKeeperClient zkc;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
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

    @Test
    public void testCreateIfNotExists() throws Exception {
        URI uri = DLMTestUtil.createDLMURI("/createIfNotExists");
        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setCreateStreamIfNotExists(false);
        String streamName = "test-stream";
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(newConf, uri);
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        try {
            writer.write(DLMTestUtil.getLogRecordInstance(1L));
            fail("Should fail to write data if stream doesn't exist.");
        } catch (IOException ioe) {
            // expected
        }
        dlm.close();

        // create the stream
        BKDistributedLogManager.createUnpartitionedStream(zkc.get(), uri, streamName);

        DistributedLogManager newDLM = factory.createDistributedLogManagerWithSharedClients(streamName);
        LogWriter newWriter = newDLM.startLogSegmentNonPartitioned();
        newWriter.write(DLMTestUtil.getLogRecordInstance(1L));
        newWriter.close();
        newDLM.close();
    }

    @Test
    public void testInvalidStreamName() throws Exception {
        assertFalse(DistributedLogManagerFactory.isReservedStreamName("test"));
        assertTrue(DistributedLogManagerFactory.isReservedStreamName(".test"));

        URI uri = DLMTestUtil.createDLMURI("/invalidStreamName");

        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        try {
            factory.createDistributedLogManagerWithSharedClients(".test1");
            fail("Should fail to create invalid stream .test");
        } catch (InvalidStreamNameException isne) {
            // expected
        }

        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients("test1");
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(1));
        writer.close();
        dlm.close();

        try {
            DistributedLogManagerFactory.createDistributedLogManager(".test2", uri);
            fail("Should fail to create invalid stream .test");
        } catch (InvalidStreamNameException isne) {
            // expected
        }

        DistributedLogManager newDLM = DistributedLogManagerFactory.createDistributedLogManager("test2", uri);
        LogWriter newWriter = newDLM.startLogSegmentNonPartitioned();
        newWriter.write(DLMTestUtil.getLogRecordInstance(1));
        newWriter.close();
        newDLM.close();

        Collection<String> streams = factory.enumerateAllLogsInNamespace();
        Set<String> streamSet = new HashSet<String>();
        streamSet.addAll(streams);

        assertEquals(2, streams.size());
        assertEquals(2, streamSet.size());
        assertTrue(streamSet.contains("test1"));
        assertTrue(streamSet.contains("test2"));

        Map<String, byte[]> streamMetadatas = factory.enumerateLogsWithMetadataInNamespace();
        assertEquals(2, streamMetadatas.size());
        assertTrue(streamMetadatas.containsKey("test1"));
        assertTrue(streamMetadatas.containsKey("test2"));

        factory.close();
    }

}
