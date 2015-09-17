package com.twitter.distributedlog;

import java.io.ByteArrayInputStream;
import java.net.URI;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestAppendOnlyStreamWriter extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestAppendOnlyStreamWriter.class);

    @Rule
    public TestName testNames = new TestName();

    @Test(timeout = 60000)
    public void basicReadAndWriteBehavior() throws Exception {
        String name = testNames.getMethodName();
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 51).getBytes();

        long txid = 1;
        AppendOnlyStreamWriter writer = dlmwrite.getAppendOnlyStreamWriter();
        writer.write(DLMTestUtil.repeatString("abc", 11).getBytes());
        writer.write(DLMTestUtil.repeatString("abc", 40).getBytes());
        writer.force(false);
        writer.close();
        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();

        byte[] bytesIn = new byte[byteStream.length];
        int read = reader.read(bytesIn, 0, 23);
        assertEquals(23, read);
        read = reader.read(bytesIn, 23, 31);
        assertEquals(read, 31);
        byte[] bytesInTemp = new byte[byteStream.length];
        read = reader.read(bytesInTemp, 0, byteStream.length);
        assertEquals(read, byteStream.length - 23 - 31);
        read = new ByteArrayInputStream(bytesInTemp).read(bytesIn, 23 + 31, byteStream.length - 23 - 31);
        assertEquals(read, byteStream.length - 23 - 31);
        assertArrayEquals(bytesIn, byteStream);
        reader.close();
        dlmreader.close();
        dlmwrite.close();
    }

    @Test(timeout = 60000)
    public void writeFutureDoesNotCompleteUntilWritePersisted() throws Exception {
        String name = testNames.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setPeriodicFlushFrequencyMilliSeconds(Integer.MAX_VALUE);
        conf.setImmediateFlushEnabled(false);

        DistributedLogManager dlmwriter = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 51).getBytes();

        // Can't reliably test the future is not completed until fsync is called, since writer.force may just
        // happen very quickly. But we can test that the mechanics of the future write and api are basically
        // correct.
        AppendOnlyStreamWriter writer = dlmwriter.getAppendOnlyStreamWriter();
        Future<DLSN> dlsnFuture = writer.write(DLMTestUtil.repeatString("abc", 11).getBytes());

        // Temp solution for PUBSUB-2555. The real problem is the fsync completes before writes are submitted, so
        // it never takes effect.
        Thread.sleep(1000);
        assertFalse(dlsnFuture.isDefined());
        writer.force(false);
        // Must not throw.
        Await.result(dlsnFuture, Duration.fromSeconds(5));
        writer.close();
        dlmwriter.close();

        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();
        byte[] bytesIn = new byte[byteStream.length];
        int read = reader.read(bytesIn, 0, 31);
        assertEquals(31, read);
        reader.close();
        dlmreader.close();
    }

    @Test(timeout = 60000)
    public void positionUpdatesOnlyAfterWriteCompletion() throws Exception {
        String name = testNames.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setPeriodicFlushFrequencyMilliSeconds(10*1000);
        conf.setImmediateFlushEnabled(false);

        DistributedLogManager dlmwriter = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 11).getBytes();

        // Can't reliably test the future is not completed until fsync is called, since writer.force may just
        // happen very quickly. But we can test that the mechanics of the future write and api are basically
        // correct.
        AppendOnlyStreamWriter writer = dlmwriter.getAppendOnlyStreamWriter();
        Future<DLSN> dlsnFuture = writer.write(byteStream);
        Thread.sleep(100);

        // Write hasn't been persisted, position better not be updated.
        assertFalse(dlsnFuture.isDefined());
        assertEquals(0, writer.position());
        writer.force(false);
        // Position guaranteed to be accurate after writer.force().
        assertEquals(byteStream.length, writer.position());

        // Close writer.
        writer.close();
        dlmwriter.close();

        // Make sure we can read it.
        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();
        byte[] bytesIn = new byte[byteStream.length];
        int read = reader.read(bytesIn, 0, byteStream.length);
        assertEquals(byteStream.length, read);
        assertEquals(byteStream.length, reader.position());
        reader.close();
        dlmreader.close();
    }

    @Test(timeout = 60000)
    public void positionDoesntUpdateBeforeWriteCompletion() throws Exception {
        String name = testNames.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration();

        // Long flush time, but we don't wait for it.
        conf.setPeriodicFlushFrequencyMilliSeconds(100*1000);
        conf.setImmediateFlushEnabled(false);
        conf.setOutputBufferSize(1024*1024);

        DistributedLogManager dlmwriter = createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 11).getBytes();

        AppendOnlyStreamWriter writer = dlmwriter.getAppendOnlyStreamWriter();
        assertEquals(0, writer.position());

        // Much much less than the flush time, small enough not to slow down tests too much, just
        // gives a little more confidence.
        Thread.sleep(500);
        Future<DLSN> dlsnFuture = writer.write(byteStream);
        assertEquals(0, writer.position());

        writer.close();
        dlmwriter.close();
    }

    @Test(timeout = 60000)
    public void positionUpdatesOnlyAfterWriteCompletionWithoutFsync() throws Exception {
        String name = testNames.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setPeriodicFlushFrequencyMilliSeconds(1*1000);
        conf.setImmediateFlushEnabled(false);
        conf.setOutputBufferSize(1024*1024);

        DistributedLogManager dlmwriter = createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 11).getBytes();

        AppendOnlyStreamWriter writer = dlmwriter.getAppendOnlyStreamWriter();
        assertEquals(0, writer.position());

        Await.result(writer.write(byteStream));
        assertEquals(33, writer.position());

        writer.close();
        dlmwriter.close();
    }

    @Test(timeout = 60000)
    public void writerStartsAtTxidZeroForEmptyStream() throws Exception {
        String name = testNames.getMethodName();
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setImmediateFlushEnabled(true);
        conf.setOutputBufferSize(1024);
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(conf, name);

        URI uri = createDLMURI("/" + name);
        BKDistributedLogManager.createLog(conf, dlm.getReaderZKC(), uri, name);

        // Log exists but is empty, better not throw.
        AppendOnlyStreamWriter writer = dlm.getAppendOnlyStreamWriter();
        byte[] byteStream = DLMTestUtil.repeatString("a", 1025).getBytes();
        Await.result(writer.write(byteStream));

        writer.close();
        dlm.close();
    }
}
