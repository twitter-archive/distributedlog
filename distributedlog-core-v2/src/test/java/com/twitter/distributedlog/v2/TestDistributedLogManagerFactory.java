package com.twitter.distributedlog.v2;

import java.net.URI;

import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.LogRecord;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestDistributedLogManagerFactory {

    @Rule
    public TestName runtime = new TestName();

    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogManagerFactory.class);

    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration().setLockTimeout(10);
    protected static LocalDLMEmulator bkutil;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;
    static int numBookies = 3;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
        zkServers = "127.0.0.1:7000";
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    private void initDlogMeta(String namespace, String un, String streamName) throws Exception {
        URI uri = DLMTestUtil.createDLMURI(namespace);
        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setZkAclId(un);
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(newConf, uri);
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (int i = 0; i < 10; i++) {
            writer.write(DLMTestUtil.getLogRecordInstance(1L));
        }
        writer.close();
        dlm.close();
        factory.close();
    }

    @Test
    public void testClientSharingOptions() throws Exception {
        URI uri = DLMTestUtil.createDLMURI("/clientSharingOptions");
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        {
            BKDistributedLogManager bkdlm1 = (BKDistributedLogManager)factory.createDistributedLogManager("perstream1",
                DistributedLogManagerFactory.ClientSharingOption.PerStreamClients);

            BKDistributedLogManager bkdlm2 = (BKDistributedLogManager)factory.createDistributedLogManager("perstream2",
                DistributedLogManagerFactory.ClientSharingOption.PerStreamClients);

            assert(bkdlm1.getReaderBKC() != bkdlm2.getReaderBKC());
            assert(bkdlm1.getWriterBKC() != bkdlm2.getWriterBKC());
            assert(bkdlm1.getReaderZKC() != bkdlm2.getReaderZKC());
            assert(bkdlm1.getWriterZKC() != bkdlm2.getWriterZKC());

        }

        {
            BKDistributedLogManager bkdlm1 = (BKDistributedLogManager)factory.createDistributedLogManager("sharedZK1",
                DistributedLogManagerFactory.ClientSharingOption.SharedZKClientPerStreamBKClient);

            BKDistributedLogManager bkdlm2 = (BKDistributedLogManager)factory.createDistributedLogManager("sharedZK2",
                DistributedLogManagerFactory.ClientSharingOption.SharedZKClientPerStreamBKClient);

            assert(bkdlm1.getReaderBKC() != bkdlm2.getReaderBKC());
            assert(bkdlm1.getWriterBKC() != bkdlm2.getWriterBKC());
            assert(bkdlm1.getReaderZKC() == bkdlm2.getReaderZKC());
            assert(bkdlm1.getWriterZKC() == bkdlm2.getWriterZKC());
        }

        {
            BKDistributedLogManager bkdlm1 = (BKDistributedLogManager)factory.createDistributedLogManager("sharedBoth1",
                DistributedLogManagerFactory.ClientSharingOption.SharedClients);

            BKDistributedLogManager bkdlm2 = (BKDistributedLogManager)factory.createDistributedLogManager("sharedBoth2",
                DistributedLogManagerFactory.ClientSharingOption.SharedClients);

            assert(bkdlm1.getReaderBKC() == bkdlm2.getReaderBKC());
            assert(bkdlm1.getWriterBKC() == bkdlm2.getWriterBKC());
            assert(bkdlm1.getReaderZKC() == bkdlm2.getReaderZKC());
            assert(bkdlm1.getWriterZKC() == bkdlm2.getWriterZKC());
        }

    }

    @Test
    public void testAclPermsZkAccessConflict() throws Exception {

        String namespace = "/" + runtime.getMethodName();
        initDlogMeta(namespace, "test-un", "test-stream");
        URI uri = DLMTestUtil.createDLMURI(namespace);

        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
            .name("unpriv")
            .uri(uri)
            .sessionTimeoutMs(2000)
            .zkAclId(null)
            .build();

        try {
            zkc.get().create(uri.getPath() + "/test-stream/test-garbage",
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            fail("write should have failed due to perms");
        } catch (KeeperException.NoAuthException ex) {
            LOG.info("caught exception trying to write with no perms", ex);
        }

        try {
            zkc.get().setData(uri.getPath() + "/test-stream", new byte[0], 0);
            fail("write should have failed due to perms");
        } catch (KeeperException.NoAuthException ex) {
            LOG.info("caught exception trying to write with no perms", ex);
        }
    }

    @Test
    public void testAclPermsZkAccessNoConflict() throws Exception {

        String namespace = "/" + runtime.getMethodName();
        initDlogMeta(namespace, "test-un", "test-stream");
        URI uri = DLMTestUtil.createDLMURI(namespace);

        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
            .name("unpriv")
            .uri(uri)
            .sessionTimeoutMs(2000)
            .zkAclId(null)
            .build();

        zkc.get().getChildren(uri.getPath() + "/test-stream", false, new Stat());
        zkc.get().getData(uri.getPath() + "/test-stream", false, new Stat());
    }

    @Test
    public void testAclModifyPermsDlmConflict() throws Exception {

        String streamName = "test-stream";

        // Reopening and writing again with the same un will succeed.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);

        try {
            // Reopening and writing again with a different un will fail.
            initDlogMeta("/" + runtime.getMethodName(), "not-test-un", streamName);
            fail("write should have failed due to perms");
        } catch (LockingException ex) {
            LOG.info("caught exception trying to write with no perms {}", ex);
            assertEquals(KeeperException.NoAuthException.class, ex.getCause().getClass());
        }

        // Should work again.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);
    }

    @Test
    public void testAclModifyPermsDlmNoConflict() throws Exception {

        String streamName = "test-stream";

        // Establish the uri.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);

        // Reopening and writing again with the same un will succeed.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);
    }

    @Test(timeout = 60000)
    public void testCreateUnpartitionedStreamWhenStreamExists() throws Exception {
        String streamName = "test-create-unpartitioned-stream-when-stream-exists";

        URI uri = DLMTestUtil.createDLMURI("/test-create-unpartitioned-stream-when-stream-exists");
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        BKContinuousLogReader reader = (BKContinuousLogReader) dlm.getInputStream(0L);

        factory.createUnpartitionedStream(streamName);
        // should not throw any exception if the stream already exists.
        factory.createUnpartitionedStream(streamName);

        // write data
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (int i = 0; i < 10; i++) {
            writer.write(DLMTestUtil.getLogRecordInstance(i));
        }
        writer.close();

        LogRecord record = reader.readNext(false);
        long expectedSeqNo = 0L;
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedSeqNo, record.getTransactionId());
            ++expectedSeqNo;

            record = reader.readNext(false);
        }
        assertEquals(10L, expectedSeqNo);

        reader.close();
    }

    @Test(timeout = 60000)
    public void testCreateUnpartitionedStream() throws Exception {
        String streamName = "test-create-unpartitioned-stream";

        URI uri = DLMTestUtil.createDLMURI("/test-create-unpartitioned-stream");
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);

        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        BKContinuousLogReader reader = (BKContinuousLogReader) dlm.getInputStream(0L);
        LogRecord record = reader.readNext(false);
        assertNull("should return null on non-existent stream", record);
        try {
            reader.getReadHandler().getInputStream(0L, true, null);
            fail("Should throw LogEmptyException when stream isn't created.");
        } catch (LogNotFoundException lee) {
            // expected
        }

        factory.createUnpartitionedStream(streamName);

        record = reader.readNext(false);
        assertNull("should return null on empty stream", record);
        Pair<ResumableBKPerStreamLogReader, Boolean> result = reader.getReadHandler().getInputStream(0L, true, null);
        assertNull("no log segment is created", result.getLeft());

        // write data
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (int i = 0; i < 10; i++) {
            writer.write(DLMTestUtil.getLogRecordInstance(i));
        }
        writer.close();

        record = reader.readNext(false);
        long expectedSeqNo = 0L;
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedSeqNo, record.getTransactionId());
            ++expectedSeqNo;

            record = reader.readNext(false);
        }
        assertEquals(10L, expectedSeqNo);

        reader.close();
    }
}
