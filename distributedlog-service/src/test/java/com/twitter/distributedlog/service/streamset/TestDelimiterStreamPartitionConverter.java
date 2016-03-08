package com.twitter.distributedlog.service.streamset;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Cases for {@link DelimiterStreamPartitionConverter}
 */
public class TestDelimiterStreamPartitionConverter {

    @Test(timeout = 20000)
    public void testNormalStream() throws Exception {
        StreamPartitionConverter converter = new DelimiterStreamPartitionConverter();
        assertEquals(new Partition("distributedlog-smoketest", 1),
                converter.convert("distributedlog-smoketest_1"));
        assertEquals(new Partition("distributedlog-smoketest-", 1),
                converter.convert("distributedlog-smoketest-_1"));
        assertEquals(new Partition("distributedlog-smoketest", 1),
                converter.convert("distributedlog-smoketest_000001"));
    }

    private void assertIdentify(String streamName, StreamPartitionConverter converter) {
        assertEquals(new Partition(streamName, 0), converter.convert(streamName));
    }

    @Test(timeout = 20000)
    public void testUnknownStream() throws Exception {
        StreamPartitionConverter converter = new DelimiterStreamPartitionConverter();
        assertIdentify("test1", converter);
        assertIdentify("test1-000001", converter);
        assertIdentify("test1_test1_000001", converter);
        assertIdentify("test1_test1", converter);
    }
}
