package com.twitter.distributedlog;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.util.Await;
import com.twitter.util.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestAppendOnlyStreamReader extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestAppendOnlyStreamReader.class);

    @Rule
    public TestName testNames = new TestName();

    // Simple test subroutine writes some records, reads some back, skips ahead, skips back.
    public void skipForwardThenSkipBack(String name, DistributedLogConfiguration conf) throws Exception {
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        long txid = 1;
        AppendOnlyStreamWriter writer = dlmwrite.getAppendOnlyStreamWriter();
        writer.write(DLMTestUtil.repeatString("abc", 5).getBytes());
        writer.write(DLMTestUtil.repeatString("abc", 5).getBytes());
        writer.write(DLMTestUtil.repeatString("def", 5).getBytes());
        writer.write(DLMTestUtil.repeatString("def", 5).getBytes());
        writer.write(DLMTestUtil.repeatString("ghi", 5).getBytes());
        writer.write(DLMTestUtil.repeatString("ghi", 5).getBytes());
        writer.force(false);
        writer.close();

        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();
        byte[] bytesIn = new byte[30];

        byte[] bytes1 = DLMTestUtil.repeatString("abc", 10).getBytes();
        byte[] bytes2 = DLMTestUtil.repeatString("def", 10).getBytes();
        byte[] bytes3 = DLMTestUtil.repeatString("ghi", 10).getBytes();

        int read = reader.read(bytesIn, 0, 30);
        assertEquals(30, read);
        assertTrue(Arrays.equals(bytes1, bytesIn));

        reader.skipTo(60);
        read = reader.read(bytesIn, 0, 30);
        assertEquals(30, read);
        assertTrue(Arrays.equals(bytes3, bytesIn));

        reader.skipTo(30);
        read = reader.read(bytesIn, 0, 30);
        assertEquals(30, read);
        assertTrue(Arrays.equals(bytes2, bytesIn));
    }

    @Test(timeout = 60000)
    public void skipToSkipsBytesWithImmediateFlush() throws Exception {
        String name = testNames.getMethodName();

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);

        skipForwardThenSkipBack(name, confLocal);
    }

    @Test(timeout = 60000)
    public void skipToSkipsBytesWithLargerLogRecords() throws Exception {
        String name = testNames.getMethodName();

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(1024*100);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(1000*60);

        skipForwardThenSkipBack(name, confLocal);
    }

    @Test(timeout = 60000)
    public void skipToSkipsBytesUntilEndOfStream() throws Exception {
        String name = testNames.getMethodName();

        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        long txid = 1;
        AppendOnlyStreamWriter writer = dlmwrite.getAppendOnlyStreamWriter();
        writer.write(DLMTestUtil.repeatString("abc", 5).getBytes());
        writer.markEndOfStream();
        writer.force(false);
        writer.close();

        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();
        byte[] bytesIn = new byte[9];

        int read = reader.read(bytesIn, 0, 9);
        assertEquals(9, read);
        assertTrue(Arrays.equals(DLMTestUtil.repeatString("abc", 3).getBytes(), bytesIn));

        assertTrue(reader.skipTo(15));

        try {
            read = reader.read(bytesIn, 0, 1);
            fail("Should have thrown");
        } catch (EndOfStreamException ex) {
        }

        assertTrue(reader.skipTo(0));

        try {
            reader.skipTo(16);
            fail("Should have thrown");
        } catch (EndOfStreamException ex) {
        }
    }

    @Test(timeout = 60000)
    public void skipToreturnsFalseIfPositionDoesNotExistYetForUnSealedStream() throws Exception {
        String name = testNames.getMethodName();

        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);

        long txid = 1;
        AppendOnlyStreamWriter writer = dlmwrite.getAppendOnlyStreamWriter();
        writer.write(DLMTestUtil.repeatString("abc", 5).getBytes());
        writer.close();

        final AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();
        byte[] bytesIn = new byte[9];

        int read = reader.read(bytesIn, 0, 9);
        assertEquals(9, read);
        assertTrue(Arrays.equals(DLMTestUtil.repeatString("abc", 3).getBytes(), bytesIn));

        assertFalse(reader.skipTo(16));
        assertFalse(reader.skipTo(16));

        AppendOnlyStreamWriter writer2 = dlmwrite.getAppendOnlyStreamWriter();
        writer2.write(DLMTestUtil.repeatString("abc", 5).getBytes());
        writer2.close();

        assertTrue(reader.skipTo(16));

        byte[] bytesIn2 = new byte[5];
        read = reader.read(bytesIn2, 0, 5);
        assertEquals(5, read);
        assertTrue(Arrays.equals("bcabc".getBytes(), bytesIn2));
    }
}
