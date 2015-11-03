package com.twitter.distributedlog.service;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.acl.DefaultAccessControlManager;
import com.twitter.distributedlog.exceptions.InternalServerException;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.stream.WriteOp;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test Case for StreamOps.
 */
public class TestStreamOp {

    static final Logger logger = LoggerFactory.getLogger(TestStreamOp.class);

    @Rule
    public TestName testName = new TestName();

    private final ThreadLocal<CRC32> requestCRC = new ThreadLocal<CRC32>() {
        @Override
        protected CRC32 initialValue() {
            return new CRC32();
        }
    };

    private WriteOp getWriteOp() {
        SettableFeature disabledFeature = new SettableFeature("", 0);
        return new WriteOp("test",
            ByteBuffer.wrap("test".getBytes()),
            new NullStatsLogger(),
            new NullStatsLogger(),
            new ServerConfiguration(),
            (byte)0,
            null,
            disabledFeature,
            DefaultAccessControlManager.INSTANCE);
    }

    @Test(timeout = 60000)
    public void testResponseFailedTwice() throws Exception {
        WriteOp writeOp = getWriteOp();
        writeOp.fail(new InternalServerException("test1"));
        writeOp.fail(new InternalServerException("test2"));

        WriteResponse response = Await.result(writeOp.result());
        assertEquals(StatusCode.INTERNAL_SERVER_ERROR, response.getHeader().getCode());
        assertEquals(ResponseUtils.exceptionToHeader(new InternalServerException("test1")), response.getHeader());
    }

    @Test(timeout = 60000)
    public void testResponseSucceededThenFailed() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        when(writer.write((LogRecord)any())).thenReturn(Future.value(new DLSN(1,2,3)));
        when(writer.getStreamName()).thenReturn("test");
        WriteOp writeOp = getWriteOp();
        writeOp.execute(writer, new Sequencer() {
            public long nextId() {
                return 0;
            }
        }, new Object());
        writeOp.fail(new InternalServerException("test2"));

        WriteResponse response = Await.result(writeOp.result());
        assertEquals(StatusCode.SUCCESS, response.getHeader().getCode());
    }
}
