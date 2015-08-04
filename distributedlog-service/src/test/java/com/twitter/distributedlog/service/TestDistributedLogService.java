package com.twitter.distributedlog.service;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.service.DistributedLogServiceImpl.Stream;
import com.twitter.distributedlog.service.DistributedLogServiceImpl.StreamStatus;
import com.twitter.distributedlog.service.config.NullStreamConfigProvider;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.stream.WriteOp;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test Case for DistributedLog Service
 */
public class TestDistributedLogService extends TestDistributedLogBase {

    static final Logger logger = LoggerFactory.getLogger(TestDistributedLogService.class);

    @Rule
    public TestName testName = new TestName();

    private ServerConfiguration serverConf;
    private DistributedLogConfiguration dlConf;
    private URI uri;
    private final CountDownLatch latch = new CountDownLatch(1);
    private DistributedLogServiceImpl service;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        dlConf = new DistributedLogConfiguration();
        dlConf.addConfiguration(conf);
        dlConf.setLockTimeout(0)
                .setOutputBufferSize(0)
                .setPeriodicFlushFrequencyMilliSeconds(10);
        serverConf = new ServerConfiguration();
        serverConf.loadConf(dlConf);
        serverConf.setServerThreads(1);
        uri = createDLMURI("/" + testName.getMethodName());
        service = createService(serverConf, dlConf, latch);
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (null != service) {
            service.shutdown();
        }
        super.teardown();
    }

    private DistributedLogConfiguration newLocalConf() {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(dlConf);
        return confLocal;
    }

    private DistributedLogServiceImpl createService(
            ServerConfiguration serverConf,
            DistributedLogConfiguration dlConf) throws Exception {
        return createService(serverConf, dlConf, new CountDownLatch(1));
    }

    private DistributedLogServiceImpl createService(
            ServerConfiguration serverConf,
            DistributedLogConfiguration dlConf,
            CountDownLatch latch) throws Exception {
        return new DistributedLogServiceImpl(
                serverConf, dlConf, new NullStreamConfigProvider(),
                uri, NullStatsLogger.INSTANCE, latch);
    }

    private Stream createStream(DistributedLogServiceImpl service,
                                String name) throws Exception {
        Stream stream = service.newStream(name);
        stream.initialize().suspendAcquiring().start();
        return stream;
    }

    private ByteBuffer createRecord(long txid) {
        return ByteBuffer.wrap(("record-" + txid).getBytes(UTF_8));
    }

    private WriteOp createWriteOp(DistributedLogServiceImpl service,
                                  String streamName,
                                  long txid) {
        ByteBuffer data = createRecord(txid);
        return service.newWriteOp(streamName, data);
    }

    @Test(timeout = 60000)
    public void testAcquireStreams() throws Exception {
        String streamName = testName.getMethodName();
        Stream s0 = createStream(service, streamName);
        DistributedLogServiceImpl service1 = createService(serverConf, dlConf);
        Stream s1 = createStream(service1, streamName);

        // create write ops
        WriteOp op0 = createWriteOp(service, streamName, 0L);
        s0.waitIfNeededAndWrite(op0);

        WriteOp op1 = createWriteOp(service1, streamName, 1L);
        s1.waitIfNeededAndWrite(op1);

        // check pending size
        assertEquals("Write Op 0 should be pending in service 0",
                1, s0.numPendingOps());
        assertEquals("Write Op 1 should be pending in service 1",
                1, s1.numPendingOps());

        // resume acquiring s0
        s0.resumeAcquiring();
        WriteResponse wr0 = Await.result(op0.result());
        assertEquals("Op 0 should succeed",
                StatusCode.SUCCESS, wr0.getHeader().getCode());
        assertEquals("Service 0 should acquire stream",
                StreamStatus.INITIALIZED, s0.getStatus());
        assertNotNull(s0.manager);
        assertNotNull(s0.writer);
        assertNull(s0.lastException);

        // resume acquiring s1
        s1.resumeAcquiring();
        WriteResponse wr1 = Await.result(op1.result());
        assertEquals("Op 1 should fail",
                StatusCode.FOUND, wr1.getHeader().getCode());
        assertEquals("Service 1 should be in BACKOFF state",
                StreamStatus.BACKOFF, s1.getStatus());
        assertNotNull(s1.manager);
        assertNull(s1.writer);
        assertNotNull(s1.lastException);
        assertTrue(s1.lastException instanceof OwnershipAcquireFailedException);

        service1.shutdown();
    }

    @Test(timeout = 60000)
    public void testCloseShouldErrorOutPendingOps() throws Exception {
        String streamName = testName.getMethodName();
        Stream s = createStream(service, streamName);

        int numWrites = 10;
        List<Future<WriteResponse>> futureList = new ArrayList<Future<WriteResponse>>(numWrites);
        for (int i = 0; i < numWrites; i++) {
            WriteOp op = createWriteOp(service, streamName, i);
            s.waitIfNeededAndWrite(op);
            futureList.add(op.result());
        }
        assertEquals(numWrites, s.numPendingOps());
        Await.result(s.requestClose("close stream"));
        assertEquals("Stream " + streamName + " is set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());
        for (int i = 0; i < numWrites; i++) {
            Future<WriteResponse> future = futureList.get(i);
            WriteResponse wr = Await.result(future);
            assertEquals("Pending op should fail with " + StatusCode.STREAM_UNAVAILABLE,
                    StatusCode.STREAM_UNAVAILABLE, wr.getHeader().getCode());
        }
    }

    @Test(timeout = 60000)
    public void testCloseTwice() throws Exception {
        String streamName = testName.getMethodName();
        Stream s = createStream(service, streamName);

        int numWrites = 10;
        List<Future<WriteResponse>> futureList = new ArrayList<Future<WriteResponse>>(numWrites);
        for (int i = 0; i < numWrites; i++) {
            WriteOp op = createWriteOp(service, streamName, i);
            s.waitIfNeededAndWrite(op);
            futureList.add(op.result());
        }
        assertEquals(numWrites, s.numPendingOps());

        final CountDownLatch deferCloseLatch = new CountDownLatch(1);
        service.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    deferCloseLatch.await();
                } catch (InterruptedException e) {
                    logger.warn("Interrupted on deferring closing : ", e);
                }
            }
        }, 0L);

        Future<Void> closeFuture0 = s.requestClose("close 0");
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSING,
                StreamStatus.CLOSING, s.getStatus());
        Future<Void> closeFuture1 = s.requestClose("close 1");
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSING,
                StreamStatus.CLOSING, s.getStatus());
        assertFalse(closeFuture0.isDefined());
        assertFalse(closeFuture1.isDefined());

        deferCloseLatch.countDown();
        Await.result(closeFuture0);
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());
        Await.result(closeFuture1);
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());

        for (int i = 0; i < numWrites; i++) {
            Future<WriteResponse> future = futureList.get(i);
            WriteResponse wr = Await.result(future);
            assertEquals("Pending op should fail with " + StatusCode.STREAM_UNAVAILABLE,
                    StatusCode.STREAM_UNAVAILABLE, wr.getHeader().getCode());
        }
    }

    @Test(timeout = 60000)
    public void testFailRequestsDuringClosing() throws Exception {
        String streamName = testName.getMethodName();
        Stream s = createStream(service, streamName);

        final CountDownLatch deferCloseLatch = new CountDownLatch(1);
        service.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    deferCloseLatch.await();
                } catch (InterruptedException e) {
                    logger.warn("Interrupted on deferring closing : ", e);
                }
            }
        }, 0L);

        Future<Void> closeFuture = s.requestClose("close");
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSING,
                StreamStatus.CLOSING, s.getStatus());
        assertFalse(closeFuture.isDefined());
        WriteOp op1 = createWriteOp(service, streamName, 0L);
        s.waitIfNeededAndWrite(op1);
        WriteResponse response1 = Await.result(op1.result());
        assertEquals("Op should fail with " + StatusCode.STREAM_UNAVAILABLE + " if it is closing",
                StatusCode.STREAM_UNAVAILABLE, response1.getHeader().getCode());

        deferCloseLatch.countDown();
        Await.result(closeFuture);
        assertEquals("Stream " + streamName + " should be set to " + StreamStatus.CLOSED,
                StreamStatus.CLOSED, s.getStatus());
        WriteOp op2 = createWriteOp(service, streamName, 1L);
        s.waitIfNeededAndWrite(op2);
        WriteResponse response2 = Await.result(op2.result());
        assertEquals("Op should fail with " + StatusCode.STREAM_UNAVAILABLE + " if it is closed",
                StatusCode.STREAM_UNAVAILABLE, response2.getHeader().getCode());
    }

    @Test(timeout = 60000)
    public void testServiceTimeout() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setServiceTimeoutMs(200)
                .setStreamProbationTimeoutMs(100)
                .setOutputBufferSize(Integer.MAX_VALUE)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(0);
        String streamName = testName.getMethodName();
        // create a new service with 200ms timeout
        DistributedLogServiceImpl localService = createService(serverConf, confLocal);

        final CountDownLatch deferCloseLatch = new CountDownLatch(1);
        localService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    deferCloseLatch.await();
                } catch (InterruptedException ie) {
                    logger.warn("Interrupted on deferring closing : ", ie);
                }
            }
        }, 0L);

        int numWrites = 10;
        List<Future<WriteResponse>> futureList = new ArrayList<Future<WriteResponse>>(numWrites);
        for (int i = 0; i < numWrites; i++) {
            futureList.add(localService.write(streamName, createRecord(i)));
        }

        assertTrue("Stream " + streamName + " should be cached",
                localService.getCachedStreams().containsKey(streamName));

        Stream s = localService.getCachedStreams().get(streamName);
        // the stream should be set CLOSING
        while (StreamStatus.CLOSING != s.getStatus()) {
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertNotNull("Writer should be initialized", s.writer);
        assertNull("No exception should be thrown", s.lastException);
        for (int i = 0; i < numWrites; i++) {
            assertFalse("Write should not fail before closing",
                    futureList.get(i).isDefined());
        }

        // resume closing
        deferCloseLatch.countDown();
        // those write ops should be aborted
        for (int i = 0; i < numWrites - 1; i++) {
            WriteResponse response = Await.result(futureList.get(i));
            assertEquals("Op should fail with " + StatusCode.BK_TRANSMIT_ERROR,
                    StatusCode.BK_TRANSMIT_ERROR, response.getHeader().getCode());
        }

        while (localService.getCachedStreams().containsKey(streamName)) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        assertFalse("Stream should be removed from cache",
                localService.getCachedStreams().containsKey(streamName));
        assertFalse("Stream should be removed from acquired cache",
                localService.getAcquiredStreams().containsKey(streamName));

        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testCloseStreamsShouldFlush() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setOutputBufferSize(Integer.MAX_VALUE)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(0);

        String streamNamePrefix = testName.getMethodName();
        DistributedLogServiceImpl localService = createService(serverConf, confLocal);

        int numStreams = 10;
        int numWrites = 10;
        List<Future<WriteResponse>> futureList =
                Lists.newArrayListWithExpectedSize(numStreams * numWrites);
        for (int i = 0; i < numStreams; i++) {
            String streamName = streamNamePrefix + "-" + i;
            for (int j = 0; j < numWrites; j++) {
                futureList.add(localService.write(streamName, createRecord(i * numWrites + j)));
            }
        }

        assertEquals("There should be " + numStreams + " streams in cache",
                numStreams, localService.getCachedStreams().size());
        while (localService.getAcquiredStreams().size() < numStreams) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        Future<List<Void>> closeResult = localService.closeStreams();
        List<Void> closedStreams = Await.result(closeResult);
        assertEquals("There should be " + numStreams + " streams closed",
                numStreams, closedStreams.size());
        // all writes should be flushed
        for (Future<WriteResponse> future : futureList) {
            WriteResponse response = Await.result(future);
            assertTrue("Op should succeed or be rejected : " + response.getHeader().getCode(),
                    StatusCode.SUCCESS == response.getHeader().getCode() ||
                    StatusCode.WRITE_EXCEPTION == response.getHeader().getCode());
        }
        assertTrue("There should be no streams in the cache",
                localService.getCachedStreams().isEmpty());
        assertTrue("There should be no streams in the acquired cache",
                localService.getAcquiredStreams().isEmpty());

        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testCloseStreamsShouldAbort() throws Exception {
        DistributedLogConfiguration confLocal = newLocalConf();
        confLocal.setOutputBufferSize(Integer.MAX_VALUE)
                .setImmediateFlushEnabled(false)
                .setPeriodicFlushFrequencyMilliSeconds(0);

        String streamNamePrefix = testName.getMethodName();
        DistributedLogServiceImpl localService = createService(serverConf, confLocal);

        int numStreams = 10;
        int numWrites = 10;
        List<Future<WriteResponse>> futureList =
                Lists.newArrayListWithExpectedSize(numStreams * numWrites);
        for (int i = 0; i < numStreams; i++) {
            String streamName = streamNamePrefix + "-" + i;
            for (int j = 0; j < numWrites; j++) {
                futureList.add(localService.write(streamName, createRecord(i * numWrites + j)));
            }
        }

        assertEquals("There should be " + numStreams + " streams in cache",
                numStreams, localService.getCachedStreams().size());
        while (localService.getAcquiredStreams().size() < numStreams) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        for (Stream s : localService.getAcquiredStreams().values()) {
            s.status = StreamStatus.FAILED;
        }

        Future<List<Void>> closeResult = localService.closeStreams();
        List<Void> closedStreams = Await.result(closeResult);
        assertEquals("There should be " + numStreams + " streams closed",
                numStreams, closedStreams.size());
        // all writes should be flushed
        for (Future<WriteResponse> future : futureList) {
            WriteResponse response = Await.result(future);
            assertTrue("Op should fail with " + StatusCode.BK_TRANSMIT_ERROR + " or be rejected : "
                    + response.getHeader().getCode(),
                    StatusCode.BK_TRANSMIT_ERROR == response.getHeader().getCode() ||
                    StatusCode.WRITE_EXCEPTION == response.getHeader().getCode());
        }
        assertTrue("There should be no streams in the cache",
                localService.getCachedStreams().isEmpty());
        assertTrue("There should be no streams in the acquired cache",
                localService.getAcquiredStreams().isEmpty());

        localService.shutdown();
    }

    @Test(timeout = 60000)
    public void testShutdown() throws Exception {
        service.shutdown();
        WriteResponse response =
                Await.result(service.write(testName.getMethodName(), createRecord(0L)));
        assertEquals("Write should fail with " + StatusCode.SERVICE_UNAVAILABLE,
                StatusCode.SERVICE_UNAVAILABLE, response.getHeader().getCode());
        assertTrue("There should be no streams created after shutdown",
                service.getCachedStreams().isEmpty());
        assertTrue("There should be no streams acquired after shutdown",
                service.getAcquiredStreams().isEmpty());
    }

}
