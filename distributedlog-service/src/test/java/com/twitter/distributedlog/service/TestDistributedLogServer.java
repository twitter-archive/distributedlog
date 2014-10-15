package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

public class TestDistributedLogServer {
    static final Logger logger = LoggerFactory.getLogger(TestDistributedLogServer.class);

    static class LocalRoutingService implements RoutingService {

        private final Map<String, SocketAddress> localAddresses =
                new HashMap<String, SocketAddress>();
        private final CopyOnWriteArrayList<RoutingListener> listeners =
                new CopyOnWriteArrayList<RoutingListener>();

        @Override
        public void startService() {
            // nop
        }

        @Override
        public void stopService() {
            // nop
        }

        @Override
        public RoutingService registerListener(RoutingListener listener) {
            listeners.add(listener);
            return this;
        }

        @Override
        public RoutingService unregisterListener(RoutingListener listener) {
            listeners.remove(listener);
            return this;
        }

        void addHost(String stream, SocketAddress address) {
            boolean notify = false;
            synchronized (this) {
                if (!localAddresses.containsKey(stream)) {
                    localAddresses.put(stream, address);
                    notify = true;
                }
            }
            if (notify) {
                for (RoutingListener listener : listeners) {
                    listener.onServerJoin(address);
                }
            }
        }

        @Override
        public synchronized SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException {
            SocketAddress address = localAddresses.get(key);
            if (null != address) {
                return address;
            }
            throw new NoBrokersAvailableException("No host available");
        }

        @Override
        public void removeHost(SocketAddress address, Throwable reason) {
            // nop
        }
    }

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setOutputBufferSize(0).setPeriodicFlushFrequencyMilliSeconds(10);
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    private static SocketAddress localAddress;
    private static LocalRoutingService routingService;
    private static DistributedLogClient dlClient;
    private static URI uri;
    private static Pair<DistributedLogServiceImpl, Server> dlServer;
    static int numBookies = 3;
    private static List<File> tmpDirs = new ArrayList<File>();

    @BeforeClass
    public static void setupCluster() throws Exception {
        File zkTmpDir = IOUtils.createTempDir("zookeeper",
                TestDistributedLogServer.class.getName());
        tmpDirs.add(zkTmpDir);
        zks = LocalBookKeeper.runZookeeper(1000, 7000, zkTmpDir);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
        uri = LocalDLMEmulator.createDLMURI("127.0.0.1:7000", "");
        dlServer = DistributedLogServer.runServer(conf, uri, new NullStatsProvider(), 7001);
        localAddress = DLSocketAddress.getSocketAddress(7001);
        routingService = new LocalRoutingService();
        dlClient = DistributedLogClientBuilder.newBuilder()
                .name("test")
                .clientId(ClientId$.MODULE$.apply("test"))
                .routingService(routingService)
                .build();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        dlClient.close();
        DistributedLogServer.closeServer(dlServer);
        bkutil.teardown();
        zks.stop();
        for (File dir : tmpDirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    @Test(timeout = 60000)
    public void testBasicWrite() throws Exception {
        String name = "dlserver-basic-write";

        routingService.addHost(name, localAddress);

        for (long i = 1; i <= 10; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        Thread.sleep(1000);

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, uri);
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

    private void runSimpleBulkWriteTest(int writeCount) throws Exception {
        String name = String.format("dlserver-bulk-write-%d", writeCount);

        routingService.addHost(name, localAddress);

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>(writeCount);
        for (long i = 1; i <= writeCount; i++) {
            writes.add(ByteBuffer.wrap(("" + i).getBytes()));
        }

        logger.debug("Write {} entries to stream {}.", writeCount, name);
        List<Future<DLSN>> futures = dlClient.writeBulk(name, writes);
        assertEquals(futures.size(), writeCount);
        for (Future<DLSN> future : futures) {
            // No throw == pass.
            DLSN dlsn = Await.result(future, Duration.fromSeconds(10));
        }

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, uri);
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

        routingService.addHost(name, localAddress);

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>();
        List<Future<DLSN>> futures = dlClient.writeBulk(name, writes);

        assertEquals(0, futures.size());
    }

    @Test(timeout = 60000)
    public void testBulkWriteNullArg() throws Exception {

        String name = String.format("dlserver-bulk-write-%s", "null");

        routingService.addHost(name, localAddress);

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>();
        writes.add(null);

        try {
            List<Future<DLSN>> futureResult = dlClient.writeBulk(name, writes);
            fail("should not have succeeded");
        } catch (NullPointerException npe) {
            ; // expected
        }
    }

    @Test(timeout = 60000)
    public void testBulkWriteEmptyBuffer() throws Exception {
        String name = String.format("dlserver-bulk-write-%s", "empty");

        routingService.addHost(name, localAddress);

        List<ByteBuffer> writes = new ArrayList<ByteBuffer>();
        writes.add(ByteBuffer.wrap(("").getBytes()));
        writes.add(ByteBuffer.wrap(("").getBytes()));
        List<Future<DLSN>> futures = dlClient.writeBulk(name, writes);
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

        routingService.addHost(name, localAddress);

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
        List<Future<DLSN>> futures = dlClient.writeBulk(name, writes);
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

        routingService.addHost(name, localAddress);

        final int writeCount = 100;
        List<ByteBuffer> writes = new ArrayList<ByteBuffer>(writeCount + 1);
        ByteBuffer buf = ByteBuffer.allocate(DistributedLogConstants.MAX_LOGRECORD_SIZE + 1);
        writes.add(buf);
        for (long i = 1; i <= writeCount; i++) {
            writes.add(ByteBuffer.wrap(("" + i).getBytes()));
        }

        List<Future<DLSN>> futures = dlClient.writeBulk(name, writes);
        validateFailedAsLogRecordTooLong(futures.get(0));
        int failed = validateAllFailedAsCancelled(futures, 1, futures.size());
        assertEquals(writeCount, failed);
    }

    @Test(timeout = 60000)
    public void testHeartbeat() throws Exception {
        String name = "dlserver-heartbeat";

        routingService.addHost(name, localAddress);

        DistributedLogClientBuilder.DistributedLogClientImpl clientImpl =
                DistributedLogClientBuilder.newBuilder()
                    .name("monitorclient")
                    .clientId(ClientId$.MODULE$.apply("monitorclient"))
                    .routingService(routingService)
                    .buildClient();

        for (long i = 1; i <= 10; i++) {
            logger.debug("Send heartbeat {} to stream {}.", i, name);
            clientImpl.check(name).get();
        }

        logger.debug("Write entry one to stream {}.", name);
        clientImpl.write(name, ByteBuffer.wrap("1".getBytes())).get();

        Thread.sleep(1000);

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, uri);
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

        routingService.addHost(name, localAddress);

        for (long i = 1; i <= 10; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        Thread.sleep(1000);

        logger.info("Fencing stream {}.", name);
        DLMTestUtil.fenceStream(conf, uri, name);
        logger.info("Fenced stream {}.", name);

        for (long i = 11; i <= 20; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        DistributedLogManager dlm = DLMTestUtil.createNewDLM(name, conf, uri);
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

        routingService.addHost(name, localAddress);

        long txid = 101;
        for (long i = 1; i <= 10; i++) {
            long curTxId = txid++;
            logger.debug("Write entry {} to stream {}.", curTxId, name);
            dlClient.write(name,
                    ByteBuffer.wrap(("" + curTxId).getBytes())).get();
        }
        dlClient.delete(name).get();
        Thread.sleep(1000);

        DistributedLogManager dlm101 = DLMTestUtil.createNewDLM(name, conf, uri);
        LogReader reader101 = dlm101.getInputStream(1);
        assertNull(reader101.readNext(false));
        reader101.close();
        dlm101.close();

        txid = 201;
        for (long i = 1; i <= 10; i++) {
            long curTxId = txid++;
            logger.debug("Write entry {} to stream {}.", curTxId, name);
            DLSN dlsn = dlClient.write(name,
                    ByteBuffer.wrap(("" + curTxId).getBytes())).get();
        }
        Thread.sleep(1000);

        DistributedLogManager dlm201 = DLMTestUtil.createNewDLM(name, conf, uri);
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

        routingService.addHost(name, localAddress);

        long txid = 1;
        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        for (int s = 1; s <= 2; s++) {
            for (long i = 1; i <= 10; i++) {
                long curTxId = txid++;
                logger.debug("Write entry {} to stream {}.", curTxId, name);
                DLSN dlsn = dlClient.write(name,
                        ByteBuffer.wrap(("" + curTxId).getBytes())).get();
                txid2DLSN.put(curTxId, dlsn);
            }
            if (s == 1) {
                dlClient.release(name).get();
            }
        }

        DLSN dlsnToDelete = txid2DLSN.get(11L);
        dlClient.truncate(name, dlsnToDelete).get();

        DistributedLogManager readDLM = DLMTestUtil.createNewDLM(name, conf, uri);
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
        assertEquals(10, numRead);
        reader.close();
        readDLM.close();
    }

}
