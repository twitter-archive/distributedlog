package com.twitter.distributedlog;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestAppendOnlyStreamReader extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestAppendOnlyStreamReader.class);

    @Rule
    public TestName testNames = new TestName();

    @Test(timeout = 60000)
    public void skipToSkipsBytesWithImmediateFlush() throws Exception {
        String name = testNames.getMethodName();
        DistributedLogManager dlmwrite = createNewDLM(conf, name);
        DistributedLogManager dlmreader = createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("xyz", 30).getBytes();

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
    public void skipToSkipsBytesWithLargerLogRecords() throws Exception {

    }
}
