package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FailpointUtils;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.acl.ZKAccessControl;
import com.twitter.distributedlog.client.DistributedLogClientImpl;
import com.twitter.distributedlog.client.routing.LocalRoutingService;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.stream.StreamManagerImpl;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.thrift.AccessControlEntry;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestDistributedLogServer extends DistributedLogServerTestCase {
    static final Logger logger = LoggerFactory.getLogger(TestDistributedLogServer.class);

    @Rule
    public TestName testName = new TestName();

    @Test(timeout = 60000)
    public void testBasicWrite() throws Exception {
        String name = "dlserver-basic-write";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        for (long i = 1; i <= 10; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        Thread.sleep(1000);

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, getUri());
        LogReader reader = dlm.getInputStream(1);
        int numRead = 0;
        LogRecord r = reader.readNext(false);
        while (null != r) {
            int i = Integer.parseInt(new String(r.getPayload()));
            assertEquals(numRead + 1, i);
            ++numRead;
            r = reader.readNext(false);
        }
        assertEquals(10, numRead);
        reader.close();
        dlm.close();
    }

    /**
     * Sanity check to make sure both checksum flag values work.
     */
    @Test(timeout = 60000)
    public void testChecksumFlag() throws Exception {
        String name = "dlserver-basic-write";
        LocalRoutingService routingService = LocalRoutingService.newBuilder().build();
        routingService.addHost(name, dlServer.getAddress());
        DistributedLogClientBuilder dlClientBuilder = DistributedLogClientBuilder.newBuilder()
            .name(name)
            .clientId(ClientId$.MODULE$.apply("test"))
            .routingService(routingService)
            .handshakeWithClientInfo(true)
            .clientBuilder(ClientBuilder.get()
                .hostConnectionLimit(1)
                .connectionTimeout(Duration.fromSeconds(1))
                .requestTimeout(Duration.fromSeconds(60)))
            .checksum(false);
        DistributedLogClient dlClient = (DistributedLogClientImpl) dlClientBuilder.build();
        Await.result(dlClient.write(name, ByteBuffer.wrap(("1").getBytes())));
        dlClient.close();

        dlClient = dlClientBuilder.checksum(true).build();
        Await.result(dlClient.write(name, ByteBuffer.wrap(("2").getBytes())));
        dlClient.close();
    }

    private void runSimpleBulkWriteTest(int writeCount) throws Exception {
        String name = String.format("dlserver-bulk-write-%d", writeCount);

        dlClient.routingService.addHost(name, dlServer.getAddress());

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>(writeCount);
        for (long i = 1; i <= writeCount; i++) {
            writes.add(ByteBuffer.wrap(("" + i).getBytes()));
        }

        logger.debug("Write {} entries to stream {}.", writeCount, name);
        List<Future<DLSN>> futures = dlClient.dlClient.writeBulk(name, writes);
        assertEquals(futures.size(), writeCount);
        for (Future<DLSN> future : futures) {
            // No throw == pass.
            DLSN dlsn = Await.result(future, Duration.fromSeconds(10));
        }

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, getUri());
        LogReader reader = dlm.getInputStream(1);
        int numRead = 0;
        LogRecord r = reader.readNext(false);
        while (null != r) {
            int i = Integer.parseInt(new String(r.getPayload()));
            assertEquals(numRead + 1, i);
            ++numRead;
            r = reader.readNext(false);
        }
        assertEquals(writeCount, numRead);
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testBulkWrite() throws Exception {
        runSimpleBulkWriteTest(100);
    }

    @Test(timeout = 60000)
    public void testBulkWriteSingleWrite() throws Exception {
        runSimpleBulkWriteTest(1);
    }

    @Test(timeout = 60000)
    public void testBulkWriteEmptyList() throws Exception {
        String name = String.format("dlserver-bulk-write-%d", 0);

        dlClient.routingService.addHost(name, dlServer.getAddress());

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>();
        List<Future<DLSN>> futures = dlClient.dlClient.writeBulk(name, writes);

        assertEquals(0, futures.size());
    }

    @Test(timeout = 60000)
    public void testBulkWriteNullArg() throws Exception {

        String name = String.format("dlserver-bulk-write-%s", "null");

        dlClient.routingService.addHost(name, dlServer.getAddress());

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>();
        writes.add(null);

        try {
            List<Future<DLSN>> futureResult = dlClient.dlClient.writeBulk(name, writes);
            fail("should not have succeeded");
        } catch (NullPointerException npe) {
            ; // expected
        }
    }

    @Test(timeout = 60000)
    public void testBulkWriteEmptyBuffer() throws Exception {
        String name = String.format("dlserver-bulk-write-%s", "empty");

        dlClient.routingService.addHost(name, dlServer.getAddress());

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>();
        writes.add(ByteBuffer.wrap(("").getBytes()));
        writes.add(ByteBuffer.wrap(("").getBytes()));
        List<Future<DLSN>> futures = dlClient.dlClient.writeBulk(name, writes);
        assertEquals(2, futures.size());
        for (Future<DLSN> future : futures) {
            // No throw == pass
            DLSN dlsn = Await.result(future, Duration.fromSeconds(10));
        }
    }

    void failDueToWrongException(Exception ex) {
        logger.info("testBulkWritePartialFailure: ", ex);
        fail(String.format("failed with wrong exception %s", ex.getClass().getName()));
    }

    int validateAllFailedAsCancelled(List<Future<DLSN>> futures, int start, int finish) {
        int failed = 0;
        for (int i = start; i < finish; i++) {
            Future<DLSN> future = futures.get(i);
            try {
                DLSN dlsn = Await.result(future, Duration.fromSeconds(10));
                fail("future should have failed!");
            } catch (DLException cre) {
                ++failed;
            } catch (Exception ex) {
                failDueToWrongException(ex);
            }
        }
        return failed;
    }

    void validateFailedAsLogRecordTooLong(Future<DLSN> future) {
        try {
            DLSN dlsn = Await.result(future, Duration.fromSeconds(10));
            fail("should have failed");
        } catch (DLException dle) {
            assertEquals(StatusCode.TOO_LARGE_RECORD, dle.getCode());
        } catch (Exception ex) {
            failDueToWrongException(ex);
        }
    }

    @Test(timeout = 60000)
    public void testBulkWritePartialFailure() throws Exception {
        String name = String.format("dlserver-bulk-write-%s", "partial-failure");

        dlClient.routingService.addHost(name, dlServer.getAddress());

        final int writeCount = 100;

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>(writeCount*2 + 1);
        for (long i = 1; i <= writeCount; i++) {
            writes.add(ByteBuffer.wrap(("" + i).getBytes()));
        }
        // Too big, will cause partial failure.
        ByteBuffer buf = ByteBuffer.allocate(DistributedLogConstants.MAX_LOGRECORD_SIZE + 1);
        writes.add(buf);
        for (long i = 1; i <= writeCount; i++) {
            writes.add(ByteBuffer.wrap(("" + i).getBytes()));
        }

        // Count succeeded.
        List<Future<DLSN>> futures = dlClient.dlClient.writeBulk(name, writes);
        int succeeded = 0;
        for (int i = 0; i < writeCount; i++) {
            Future<DLSN> future = futures.get(i);
            try {
                DLSN dlsn = Await.result(future, Duration.fromSeconds(10));
                ++succeeded;
            } catch (Exception ex) {
                failDueToWrongException(ex);
            }
        }

        validateFailedAsLogRecordTooLong(futures.get(writeCount));
        int failed = validateAllFailedAsCancelled(futures, writeCount+1, futures.size());

        assertEquals(writeCount, succeeded);
        assertEquals(writeCount, failed);
    }

    @Test(timeout = 60000)
    public void testBulkWriteTotalFailureFirstWriteFailed() throws Exception {
        String name = String.format("dlserver-bulk-write-%s", "first-write-failed");

        dlClient.routingService.addHost(name, dlServer.getAddress());

        final int writeCount = 100;
        List<ByteBuffer> writes = new ArrayList<ByteBuffer>(writeCount + 1);
        ByteBuffer buf = ByteBuffer.allocate(DistributedLogConstants.MAX_LOGRECORD_SIZE + 1);
        writes.add(buf);
        for (long i = 1; i <= writeCount; i++) {
            writes.add(ByteBuffer.wrap(("" + i).getBytes()));
        }

        List<Future<DLSN>> futures = dlClient.dlClient.writeBulk(name, writes);
        validateFailedAsLogRecordTooLong(futures.get(0));
        int failed = validateAllFailedAsCancelled(futures, 1, futures.size());
        assertEquals(writeCount, failed);
    }

    @Test(timeout = 60000)
    public void testBulkWriteTotalFailureLostLock() throws Exception {
        String name = String.format("dlserver-bulk-write-%s", "lost-lock");

        dlClient.routingService.addHost(name, dlServer.getAddress());

        final int writeCount = 8;
        List<ByteBuffer> writes = new ArrayList<ByteBuffer>(writeCount + 1);
        ByteBuffer buf = ByteBuffer.allocate(8);
        writes.add(buf);
        for (long i = 1; i <= writeCount; i++) {
            writes.add(ByteBuffer.wrap(("" + i).getBytes()));
        }
        // Warm it up with a write.
        Await.result(dlClient.dlClient.write(name, ByteBuffer.allocate(8)));

        // Failpoint a lost lock, make sure the failure gets promoted to an operation failure.
        DistributedLogServiceImpl svcImpl = (DistributedLogServiceImpl) dlServer.dlServer.getLeft();
        try {
            FailpointUtils.setFailpoint(
                FailpointUtils.FailPointName.FP_WriteInternalLostLock,
                FailpointUtils.FailPointActions.FailPointAction_Default
            );
            Future<BulkWriteResponse> futures = svcImpl.writeBulkWithContext(name, writes, new WriteContext());
            assertEquals(StatusCode.LOCKING_EXCEPTION, Await.result(futures).header.code);
        } finally {
            FailpointUtils.removeFailpoint(
                FailpointUtils.FailPointName.FP_WriteInternalLostLock
            );
        }
    }

    @Test(timeout = 60000)
    public void testHeartbeat() throws Exception {
        String name = "dlserver-heartbeat";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        for (long i = 1; i <= 10; i++) {
            logger.debug("Send heartbeat {} to stream {}.", i, name);
            dlClient.dlClient.check(name).get();
        }

        logger.debug("Write entry one to stream {}.", name);
        dlClient.dlClient.write(name, ByteBuffer.wrap("1".getBytes())).get();

        Thread.sleep(1000);

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, getUri());
        LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
        int numRead = 0;
        // eid=0 => control records
        // other 9 heartbeats will not trigger writing any control records.
        // eid=1 => user entry
        long startEntryId = 1;
        LogRecordWithDLSN r = reader.readNext(false);
        while (null != r) {
            int i = Integer.parseInt(new String(r.getPayload()));
            assertEquals(numRead + 1, i);
            assertEquals(r.getDlsn().compareTo(new DLSN(1, startEntryId, 0)), 0);
            ++numRead;
            ++startEntryId;
            r = reader.readNext(false);
        }
        assertEquals(1, numRead);
    }

    @Test(timeout = 60000)
    public void testFenceWrite() throws Exception {
        String name = "dlserver-fence-write";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        for (long i = 1; i <= 10; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        Thread.sleep(1000);

        logger.info("Fencing stream {}.", name);
        DLMTestUtil.fenceStream(conf, getUri(), name);
        logger.info("Fenced stream {}.", name);

        for (long i = 11; i <= 20; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, getUri());
        LogReader reader = dlm.getInputStream(1);
        int numRead = 0;
        LogRecord r = reader.readNext(false);
        while (null != r) {
            int i = Integer.parseInt(new String(r.getPayload()));
            assertEquals(numRead + 1, i);
            ++numRead;
            r = reader.readNext(false);
        }
        assertEquals(20, numRead);
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testDeleteStream() throws Exception {
        String name = "dlserver-delete-stream";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        long txid = 101;
        for (long i = 1; i <= 10; i++) {
            long curTxId = txid++;
            logger.debug("Write entry {} to stream {}.", curTxId, name);
            dlClient.dlClient.write(name,
                    ByteBuffer.wrap(("" + curTxId).getBytes())).get();
        }

        checkStream(1, 1, 1, name, dlServer.getAddress(), true, true);

        dlClient.dlClient.delete(name).get();

        checkStream(0, 0, 0, name, dlServer.getAddress(), false, false);

        Thread.sleep(1000);

        DistributedLogManager dlm101 = DLMTestUtil.createNewDLM(name, conf, getUri());
        LogReader reader101 = dlm101.getInputStream(1);
        assertNull(reader101.readNext(false));
        reader101.close();
        dlm101.close();

        txid = 201;
        for (long i = 1; i <= 10; i++) {
            long curTxId = txid++;
            logger.debug("Write entry {} to stream {}.", curTxId, name);
            DLSN dlsn = dlClient.dlClient.write(name,
                    ByteBuffer.wrap(("" + curTxId).getBytes())).get();
        }
        Thread.sleep(1000);

        DistributedLogManager dlm201 = DLMTestUtil.createNewDLM(name, conf, getUri());
        LogReader reader201 = dlm201.getInputStream(1);
        int numRead = 0;
        int curTxId = 201;
        LogRecord r = reader201.readNext(false);
        while (null != r) {
            int i = Integer.parseInt(new String(r.getPayload()));
            assertEquals(curTxId++, i);
            ++numRead;
            r = reader201.readNext(false);
        }
        assertEquals(10, numRead);
        reader201.close();
        dlm201.close();
    }

    @Test(timeout = 60000)
    public void testTruncateStream() throws Exception {
        String name = "dlserver-truncate-stream";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        long txid = 1;
        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        for (int s = 1; s <= 3; s++) {
            for (long i = 1; i <= 10; i++) {
                long curTxId = txid++;
                logger.debug("Write entry {} to stream {}.", curTxId, name);
                DLSN dlsn = dlClient.dlClient.write(name,
                        ByteBuffer.wrap(("" + curTxId).getBytes())).get();
                txid2DLSN.put(curTxId, dlsn);
            }
            if (s <= 2) {
                dlClient.dlClient.release(name).get();
            }
        }

        DLSN dlsnToDelete = txid2DLSN.get(21L);
        dlClient.dlClient.truncate(name, dlsnToDelete).get();

        DistributedLogManager readDLM = DLMTestUtil.createNewDLM(name, conf, getUri());
        LogReader reader = readDLM.getInputStream(1);
        int numRead = 0;
        int curTxId = 11;
        LogRecord r = reader.readNext(false);
        while (null != r) {
            int i = Integer.parseInt(new String(r.getPayload()));
            assertEquals(curTxId++, i);
            ++numRead;
            r = reader.readNext(false);
        }
        assertEquals(20, numRead);
        reader.close();
        readDLM.close();
    }

    @Test(timeout = 60000)
    public void testRequestDenied() throws Exception {
        String name = "request-denied";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        AccessControlEntry ace = new AccessControlEntry();
        ace.setDenyWrite(true);
        ZooKeeperClient zkc = ZooKeeperClientBuilder
                .newBuilder()
                .uri(getUri())
                .connectionTimeoutMs(60000)
                .sessionTimeoutMs(60000)
                .zkAclId(null)
                .build();
        DistributedLogNamespace dlNamespace = dlServer.dlServer.getLeft().getDistributedLogNamespace();
        BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, getUri());
        String zkPath = getUri().getPath() + "/" + bkdlConfig.getACLRootPath() + "/" + name;
        ZKAccessControl accessControl = new ZKAccessControl(ace, zkPath);
        accessControl.create(zkc);

        AccessControlManager acm = dlNamespace.createAccessControlManager();
        while (acm.allowWrite(name)) {
            Thread.sleep(100);
        }

        try {
            Await.result(dlClient.dlClient.write(name, ByteBuffer.wrap("1".getBytes(UTF_8))));
            fail("Should fail with request denied exception");
        } catch (DLException dle) {
            assertEquals(StatusCode.REQUEST_DENIED, dle.getCode());
        }
    }

    @Test(timeout = 60000)
    public void testNoneStreamNameRegex() throws Exception {
        String streamNamePrefix = "none-stream-name-regex-";
        int numStreams = 5;
        Set<String> streams = new HashSet<String>();

        for (int i = 0; i < numStreams; i++) {
            streams.add(streamNamePrefix + i);
        }
        testStreamNameRegex(streams, ".*", streams);
    }

    @Test(timeout = 60000)
    public void testStreamNameRegex() throws Exception {
        String streamNamePrefix = "stream-name-regex-";
        int numStreams = 5;
        Set<String> streams = new HashSet<String>();
        Set<String> expectedStreams = new HashSet<String>();
        String streamNameRegex = streamNamePrefix + "1";

        for (int i = 0; i < numStreams; i++) {
            streams.add(streamNamePrefix + i);
        }
        expectedStreams.add(streamNamePrefix + "1");

        testStreamNameRegex(streams, streamNameRegex, expectedStreams);
    }

    private void testStreamNameRegex(Set<String> streams, String streamNameRegex,
                                     Set<String> expectedStreams)
            throws Exception {
        for (String streamName : streams) {
            dlClient.routingService.addHost(streamName, dlServer.getAddress());
            Await.result(dlClient.dlClient.write(streamName,
                    ByteBuffer.wrap(streamName.getBytes(UTF_8))));
        }

        DLClient client = createDistributedLogClient("test-stream-name-regex", streamNameRegex);
        try {
            client.routingService.addHost("unknown", dlServer.getAddress());
            client.handshake();
            Map<SocketAddress, Set<String>> distribution =
                    client.dlClient.getStreamOwnershipDistribution();
            assertEquals(1, distribution.size());
            Set<String> cachedStreams = distribution.values().iterator().next();
            assertNotNull(cachedStreams);
            assertEquals(expectedStreams.size(), cachedStreams.size());

            for (String streamName : cachedStreams) {
                assertTrue(expectedStreams.contains(streamName));
            }
        } finally {
            client.shutdown();
        }
    }

    @Test(timeout = 60000)
    public void testReleaseStream() throws Exception {
        String name = "dlserver-release-stream";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        Await.result(dlClient.dlClient.write(name, ByteBuffer.wrap("1".getBytes(UTF_8))));
        checkStream(1, 1, 1, name, dlServer.getAddress(), true, true);

        // release the stream
        Await.result(dlClient.dlClient.release(name));
        checkStream(0, 0, 0, name, dlServer.getAddress(), false, false);
    }

    @Test(timeout = 60000)
    public void testAcceptNewStream() throws Exception {
        String name = "dlserver-accept-new-stream";

        dlClient.routingService.addHost(name, dlServer.getAddress());
        dlClient.routingService.setAllowRetrySameHost(false);

        Await.result(dlClient.dlClient.setAcceptNewStream(false));

        try {
            Await.result(dlClient.dlClient.write(name, ByteBuffer.wrap("1".getBytes(UTF_8))));
            fail("Should fail because the proxy couldn't accept new stream");
        } catch (NoBrokersAvailableException nbae) {
            // expected
        }
        checkStream(0, 0, 0, name, dlServer.getAddress(), false, false);

        Await.result(dlServer.dlServer.getLeft().setAcceptNewStream(true));
        Await.result(dlClient.dlClient.write(name, ByteBuffer.wrap("1".getBytes(UTF_8))));
        checkStream(1, 1, 1, name, dlServer.getAddress(), true, true);
    }

    private void checkStream(int expectedNumProxiesInClient, int expectedClientCacheSize, int expectedServerCacheSize,
                             String name, SocketAddress owner, boolean existedInServer, boolean existedInClient) {
        Map<SocketAddress, Set<String>> distribution = dlClient.dlClient.getStreamOwnershipDistribution();
        assertEquals(expectedNumProxiesInClient, distribution.size());

        if (expectedNumProxiesInClient > 0) {
            Map.Entry<SocketAddress, Set<String>> localEntry =
                    distribution.entrySet().iterator().next();
            assertEquals(owner, localEntry.getKey());
            assertEquals(expectedClientCacheSize, localEntry.getValue().size());
            assertEquals(existedInClient, localEntry.getValue().contains(name));
        }


        StreamManagerImpl streamManager = (StreamManagerImpl) dlServer.dlServer.getKey().getStreamManager();
        Set<String> cachedStreams = streamManager.getCachedStreams().keySet();
        Set<String> acquiredStreams = streamManager.getCachedStreams().keySet();

        assertEquals(expectedServerCacheSize, cachedStreams.size());
        assertEquals(existedInServer, cachedStreams.contains(name));
        assertEquals(expectedServerCacheSize, acquiredStreams.size());
        assertEquals(existedInServer, acquiredStreams.contains(name));
    }

}
