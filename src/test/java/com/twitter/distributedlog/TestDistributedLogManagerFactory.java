package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;

import static org.junit.Assert.*;

public class TestDistributedLogManagerFactory extends TestDistributedLogBase {

    @Rule
    public TestName runtime = new TestName();

    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogManagerFactory.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setEnableLedgerAllocatorPool(true).setLedgerAllocatorPoolName("test");

    private ZooKeeperClient zooKeeperClient;

    @Before
    public void setup() throws Exception {
        zooKeeperClient =
            ZooKeeperClientBuilder.newBuilder()
                .uri(DLMTestUtil.createDLMURI("/"))
                .zkAclId(null)
                .sessionTimeoutMs(10000).build();
    }

    @After
    public void teardown() throws Exception {
        zooKeeperClient.close();
    }

    @Test
    public void testCreateIfNotExists() throws Exception {
        URI uri = DLMTestUtil.createDLMURI("/" + runtime.getMethodName());
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
        BKDistributedLogManager.createUnpartitionedStream(conf, zooKeeperClient, uri, streamName);

        DistributedLogManager newDLM = factory.createDistributedLogManagerWithSharedClients(streamName);
        LogWriter newWriter = newDLM.startLogSegmentNonPartitioned();
        newWriter.write(DLMTestUtil.getLogRecordInstance(1L));
        newWriter.close();
        newDLM.close();
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
    public void testInvalidStreamName() throws Exception {
        assertFalse(DistributedLogManagerFactory.isReservedStreamName("test"));
        assertTrue(DistributedLogManagerFactory.isReservedStreamName(".test"));

        URI uri = DLMTestUtil.createDLMURI("/" + runtime.getMethodName());

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

    @Test(timeout = 60000)
    public void testNamespaceListener() throws Exception {
        URI uri = DLMTestUtil.createDLMURI("/" + runtime.getMethodName());
        zooKeeperClient.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(conf, uri);
        final CountDownLatch[] latches = new CountDownLatch[3];
        for (int i = 0; i < 3; i++) {
            latches[i] = new CountDownLatch(1);
        }
        final AtomicInteger numUpdates = new AtomicInteger(0);
        final AtomicInteger numFailures = new AtomicInteger(0);
        final AtomicReference<Collection<String>> receivedStreams = new AtomicReference<Collection<String>>(null);
        factory.registerNamespaceListener(new NamespaceListener() {
            @Override
            public void onStreamsChanged(Collection<String> streams) {
                int updates = numUpdates.incrementAndGet();
                if (streams.size() != updates - 1) {
                    numFailures.incrementAndGet();
                }

                receivedStreams.set(streams);
                latches[updates - 1].countDown();
            }
        });
        latches[0].await();
        BKDistributedLogManager.createUnpartitionedStream(conf, zooKeeperClient, uri, "test1");
        latches[1].await();
        BKDistributedLogManager.createUnpartitionedStream(conf, zooKeeperClient, uri, "test2");
        latches[2].await();
        assertEquals(0, numFailures.get());
        assertNotNull(receivedStreams.get());
        Set<String> streamSet = new HashSet<String>();
        streamSet.addAll(receivedStreams.get());
        assertEquals(2, receivedStreams.get().size());
        assertEquals(2, streamSet.size());
        assertTrue(streamSet.contains("test1"));
        assertTrue(streamSet.contains("test2"));
    }

    private void initDlogMeta(String namespace, String un, String streamName) throws Exception {
        URI uri = DLMTestUtil.createDLMURI(namespace);
        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setCreateStreamIfNotExists(true);
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

    static void validateBadAllocatorConfiguration(DistributedLogConfiguration conf, URI uri) throws Exception {
        try {
            DistributedLogManagerFactory.validateAndGetFullLedgerAllocatorPoolPath(conf, uri);
            fail("Should throw exception when bad allocator configuration provided");
        } catch (IOException ioe) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testValidateAndGetFullLedgerAllocatorPoolPath() throws Exception {
        DistributedLogConfiguration testConf = new DistributedLogConfiguration();
        testConf.setEnableLedgerAllocatorPool(true);

        String namespace = "/" + runtime.getMethodName();
        URI uri = DLMTestUtil.createDLMURI(namespace);

        testConf.setLedgerAllocatorPoolName("test");

        testConf.setLedgerAllocatorPoolPath("test");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath(".");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath("..");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath("./");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath(".test/");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath(".test");
        testConf.setLedgerAllocatorPoolName(null);
        validateBadAllocatorConfiguration(testConf, uri);
    }
}
