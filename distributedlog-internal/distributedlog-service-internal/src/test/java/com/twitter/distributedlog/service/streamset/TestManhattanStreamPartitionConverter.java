package com.twitter.distributedlog.service.streamset;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestManhattanStreamPartitionConverter {
    @Test(timeout = 20000)
    public void testNormalStream() throws Exception {
        StreamPartitionConverter converter = new ManhattanStreamPartitionConverter();
        assertEquals(new Partition("test1", 23), converter.convert("manhattan-xdc-rlog-test1-23"));
        assertEquals(new Partition("test1", 23), converter.convert("manhattan-rlog-test1-23"));
    }

    private void assertIdentity(String streamName, StreamPartitionConverter converter) {
        assertEquals(new Partition(streamName, 0), converter.convert(streamName));
    }

    @Test(timeout = 20000)
    public void testUnknownStream() throws Exception {
        StreamPartitionConverter converter = new ManhattanStreamPartitionConverter();
        assertIdentity("test1", converter);
        assertIdentity("test1_000001", converter);
        assertIdentity("manhattan_test1_00001", converter);
        assertIdentity("blah_test1_000001", converter);
        assertIdentity("manhattan_xdc_test1_000001", converter);
    }
}
