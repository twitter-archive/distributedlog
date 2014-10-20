package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestDistributedLogServer extends DistributedLogServerTestCase {
    static final Logger logger = LoggerFactory.getLogger(TestDistributedLogServer.class);

    @Test(timeout = 60000)
    public void testBasicWrite() throws Exception {
        String name = "dlserver-basic-write";

        dlClient.routingService.addHost(name, dlServer.getAddress());

        for (long i = 1; i <= 10; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
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

        dlClient.routingService.addHost(name, dlServer.getAddress());

        for (long i = 1; i <= 10; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
        }

        Thread.sleep(1000);

        logger.info("Fencing stream {}.", name);
        DLMTestUtil.fenceStream(conf, uri, name);
        logger.info("Fenced stream {}.", name);

        for (long i = 11; i <= 20; i++) {
            logger.debug("Write entry {} to stream {}.", i, name);
            dlClient.dlClient.write(name, ByteBuffer.wrap(("" + i).getBytes())).get();
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

        DistributedLogManager dlm101 = DLMTestUtil.createNewDLM(name, conf, uri);
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

        dlClient.routingService.addHost(name, dlServer.getAddress());

        long txid = 1;
        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        for (int s = 1; s <= 2; s++) {
            for (long i = 1; i <= 10; i++) {
                long curTxId = txid++;
                logger.debug("Write entry {} to stream {}.", curTxId, name);
                DLSN dlsn = dlClient.dlClient.write(name,
                        ByteBuffer.wrap(("" + curTxId).getBytes())).get();
                txid2DLSN.put(curTxId, dlsn);
            }
            if (s == 1) {
                dlClient.dlClient.release(name).get();
            }
        }

        DLSN dlsnToDelete = txid2DLSN.get(11L);
        dlClient.dlClient.truncate(name, dlsnToDelete).get();

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

        Set<String> cachedStreams = dlServer.dlServer.getKey().getCachedStreams().keySet();
        Set<String> acquiredStreams = dlServer.dlServer.getKey().getCachedStreams().keySet();

        assertEquals(expectedServerCacheSize, cachedStreams.size());
        assertEquals(existedInServer, cachedStreams.contains(name));
        assertEquals(expectedServerCacheSize, acquiredStreams.size());
        assertEquals(existedInServer, acquiredStreams.contains(name));
    }

}
