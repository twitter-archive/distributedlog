package com.twitter.distributedlog.client;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordSet;
import com.twitter.distributedlog.LogRecordSetBuffer;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.finagle.IndividualRequestTimeoutException;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Test {@link DistributedLogMultiStreamWriter}
 */
public class TestDistributedLogMultiStreamWriter {

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testBuildWithNullStreams() throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .build();
    }

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testBuildWithEmptyStreamList() throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.<String>newArrayList())
                .build();
    }

    @Test(timeout = 20000, expected = NullPointerException.class)
    public void testBuildWithNullClient() throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .build();
    }

    @Test(timeout = 20000, expected = NullPointerException.class)
    public void testBuildWithNullCodec() throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(null)
                .build();
    }

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testBuildWithInvalidSpeculativeSettings1()
            throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(-1)
                .build();
    }

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testBuildWithInvalidSpeculativeSettings2()
            throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(5)
                .build();
    }

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testBuildWithInvalidSpeculativeSettings3()
            throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(-1)
                .build();
    }

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testBuildWithInvalidSpeculativeSettings4()
            throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(10)
                .build();
    }

    @Test(timeout = 20000)
    public void testBuildMultiStreamWriter()
            throws Exception {
        DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(50)
                .build();
        assertTrue(true);
    }

    @Test(timeout = 20000)
    public void testBuildWithPeriodicalFlushEnabled() throws Exception {
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(50)
                .flushIntervalMs(1000)
                .scheduler(executorService)
                .build();
        verify(executorService, times(1)).scheduleAtFixedRate(writer, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 20000)
    public void testBuildWithPeriodicalFlushDisabled() throws Exception {
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(mock(DistributedLogClient.class))
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(50)
                .flushIntervalMs(0)
                .scheduler(executorService)
                .build();
        verify(executorService, times(0)).scheduleAtFixedRate(writer, 1000, 1000, TimeUnit.MILLISECONDS);
        writer.close();
    }

    @Test(timeout = 20000)
    public void testFlushWhenBufferIsFull() throws Exception {
        DistributedLogClient client = mock(DistributedLogClient.class);
        when(client.writeRecordSet((String) any(), (LogRecordSetBuffer) any()))
                .thenReturn(Future.value(new DLSN(1L, 1L, 999L)));

        ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(client)
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(100000)
                .maxSpeculativeTimeoutMs(200000)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(500000)
                .flushIntervalMs(0)
                .bufferSize(0)
                .scheduler(executorService)
                .build();

        ByteBuffer buffer = ByteBuffer.wrap("test".getBytes(UTF_8));
        writer.write(buffer);

        verify(client, times(1)).writeRecordSet((String) any(), (LogRecordSetBuffer) any());

        writer.close();
    }

    @Test(timeout = 20000)
    public void testFlushWhenExceedMaxLogRecordSetSize()
            throws Exception {
        DistributedLogClient client = mock(DistributedLogClient.class);
        when(client.writeRecordSet((String) any(), (LogRecordSetBuffer) any()))
                .thenReturn(Future.value(new DLSN(1L, 1L, 999L)));
        ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(client)
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(100000)
                .maxSpeculativeTimeoutMs(200000)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(500000)
                .flushIntervalMs(0)
                .bufferSize(Integer.MAX_VALUE)
                .scheduler(executorService)
                .build();

        byte[] data = new byte[LogRecord.MAX_LOGRECORD_SIZE - 3 * 100];
        ByteBuffer buffer1 = ByteBuffer.wrap(data);
        writer.write(buffer1);
        verify(client, times(0)).writeRecordSet((String) any(), (LogRecordSetBuffer) any());
        LogRecordSet.Writer recordSetWriter1 = writer.getLogRecordSetWriter();
        assertEquals(1, recordSetWriter1.getNumRecords());
        assertEquals(LogRecordSet.HEADER_LEN + 4 + data.length, recordSetWriter1.getNumBytes());

        ByteBuffer buffer2 = ByteBuffer.wrap(data);
        writer.write(buffer2);
        verify(client, times(1)).writeRecordSet((String) any(), (LogRecordSetBuffer) any());
        LogRecordSet.Writer recordSetWriter2 = writer.getLogRecordSetWriter();
        assertEquals(1, recordSetWriter2.getNumRecords());
        assertEquals(LogRecordSet.HEADER_LEN + 4 + data.length, recordSetWriter2.getNumBytes());
        assertTrue(recordSetWriter1 != recordSetWriter2);

        writer.close();
    }

    @Test(timeout = 20000)
    public void testWriteTooLargeRecord() throws Exception {
        DistributedLogClient client = mock(DistributedLogClient.class);
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(client)
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(100000)
                .maxSpeculativeTimeoutMs(200000)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(5000000)
                .flushIntervalMs(0)
                .bufferSize(0)
                .build();

        byte[] data = new byte[LogRecord.MAX_LOGRECORD_SIZE + 10];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Future<DLSN> writeFuture = writer.write(buffer);
        assertTrue(writeFuture.isDefined());
        try {
            Await.result(writeFuture);
            fail("Should fail on writing too long record");
        } catch (LogRecordTooLongException lrtle) {
            // expected
        }
        writer.close();
    }

    @Test(timeout = 20000)
    public void testSpeculativeWrite() throws Exception {
        DistributedLogClient client = mock(DistributedLogClient.class);
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(client)
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(5000000)
                .flushIntervalMs(0)
                .bufferSize(0)
                .build();

        final String secondStream = writer.getStream(1);

        final DLSN dlsn = new DLSN(99L, 88L, 0L);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                String stream = (String) arguments[0];
                if (stream.equals(secondStream)) {
                    return Future.value(dlsn);
                } else {
                    return new Promise<DLSN>();
                }
            }
        }).when(client).writeRecordSet((String) any(), (LogRecordSetBuffer) any());

        byte[] data = "test-test".getBytes(UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Future<DLSN> writeFuture = writer.write(buffer);
        DLSN writeDLSN = Await.result(writeFuture);
        assertEquals(dlsn, writeDLSN);
        writer.close();
    }

    @Test(timeout = 20000)
    public void testPeriodicalFlush() throws Exception {
        DistributedLogClient client = mock(DistributedLogClient.class);
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(client)
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(5000000)
                .flushIntervalMs(10)
                .bufferSize(Integer.MAX_VALUE)
                .build();

        final DLSN dlsn = new DLSN(99L, 88L, 0L);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return Future.value(dlsn);
            }
        }).when(client).writeRecordSet((String) any(), (LogRecordSetBuffer) any());

        byte[] data = "test-test".getBytes(UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Future<DLSN> writeFuture = writer.write(buffer);
        DLSN writeDLSN = Await.result(writeFuture);
        assertEquals(dlsn, writeDLSN);
        writer.close();
    }

    @Test(timeout = 20000)
    public void testFailRequestAfterRetriedAllStreams() throws Exception {
        DistributedLogClient client = mock(DistributedLogClient.class);
        when(client.writeRecordSet((String) any(), (LogRecordSetBuffer) any()))
                .thenReturn(new Promise<DLSN>());
        DistributedLogMultiStreamWriter writer = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList("stream1", "stream2"))
                .client(client)
                .compressionCodec(CompressionCodec.Type.LZ4)
                .firstSpeculativeTimeoutMs(10)
                .maxSpeculativeTimeoutMs(20)
                .speculativeBackoffMultiplier(2)
                .requestTimeoutMs(5000000)
                .flushIntervalMs(10)
                .bufferSize(Integer.MAX_VALUE)
                .build();

        byte[] data = "test-test".getBytes(UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Future<DLSN> writeFuture = writer.write(buffer);
        try {
            Await.result(writeFuture);
            fail("Should fail the request after retries all streams");
        } catch (IndividualRequestTimeoutException e) {
            long timeoutMs = e.timeout().inMilliseconds();
            assertTrue(timeoutMs >= (10 + 20) && timeoutMs < 5000000);
        }
        writer.close();
    }
}
