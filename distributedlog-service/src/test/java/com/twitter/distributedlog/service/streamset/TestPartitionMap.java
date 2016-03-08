package com.twitter.distributedlog.service.streamset;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test {@link PartitionMap}
 */
public class TestPartitionMap {

    @Test(timeout = 20000)
    public void testAddPartitionNegativeMax() {
        PartitionMap map = new PartitionMap();
        for (int i = 0; i < 20; i++) {
            assertTrue(map.addPartition(new Partition("test", i), -1));
        }
    }

    @Test(timeout = 20000)
    public void testAddPartitionMultipleTimes() {
        PartitionMap map = new PartitionMap();
        for (int i = 0; i < 20; i++) {
            assertTrue(map.addPartition(new Partition("test", 0), 3));
        }
    }

    @Test(timeout = 20000)
    public void testAddPartition() {
        PartitionMap map = new PartitionMap();
        for (int i = 0; i < 3; i++) {
            assertTrue(map.addPartition(new Partition("test", i), 3));
        }
        for (int i = 3; i < 20; i++) {
            assertFalse(map.addPartition(new Partition("test", i), 3));
        }
    }

    @Test(timeout = 20000)
    public void testRemovePartition() {
        PartitionMap map = new PartitionMap();
        for (int i = 0; i < 3; i++) {
            assertTrue(map.addPartition(new Partition("test", i), 3));
        }
        assertFalse(map.addPartition(new Partition("test", 3), 3));
        assertFalse(map.removePartition(new Partition("test", 3)));
        assertTrue(map.removePartition(new Partition("test", 0)));
        assertTrue(map.addPartition(new Partition("test", 3), 3));
    }
}
