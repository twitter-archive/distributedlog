/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
