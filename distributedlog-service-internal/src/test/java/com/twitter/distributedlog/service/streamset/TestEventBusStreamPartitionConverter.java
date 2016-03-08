package com.twitter.distributedlog.service.streamset;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestEventBusStreamPartitionConverter {
    @Test(timeout = 20000)
    public void testNormalStream() throws Exception {
        StreamPartitionConverter converter = new EventBusStreamPartitionConverter();
        assertEquals(new Partition("test1", 1), converter.convert("eventbus_test1_000001"));
    }

    @Test(timeout = 20000)
    public void testDarkStream() throws Exception {
        StreamPartitionConverter converter = new EventBusStreamPartitionConverter();
        assertEquals(new Partition("test1", 1), converter.convert("__dark_eventbus_test1_000001"));
    }

    private void assertIdentity(String streamName, StreamPartitionConverter converter) {
        assertEquals(new Partition(streamName, 0), converter.convert(streamName));
    }

    @Test(timeout = 20000)
    public void testUnknownStream() throws Exception {
        StreamPartitionConverter converter = new EventBusStreamPartitionConverter();
        assertIdentity("test1", converter);
        assertIdentity("test1_000001", converter);
        assertIdentity("eventbus_test1_00001", converter);
        assertIdentity("blah_test1_000001", converter);
        assertIdentity("_dark_eventbus_test1_000001", converter);
        assertIdentity("eventbus_test1_00a001", converter);
        assertIdentity("eventbus__00a001", converter);
    }
}
