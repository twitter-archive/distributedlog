package com.twitter.distributedlog.service.streamset;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestIdentityStreamPartitionConverter {

    @Test(timeout = 20000)
    public void testIdentityConverter() {
        String streamName = "test-identity-converter";

        IdentityStreamPartitionConverter converter =
                new IdentityStreamPartitionConverter();

        Partition p0 = converter.convert(streamName);
        assertEquals(new Partition(streamName, 0), p0);

        Partition p1 = converter.convert(streamName);
        assertTrue(p0 == p1);
    }
}
