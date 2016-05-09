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
package com.twitter.distributedlog.service.balancer;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestBalancerUtils {

    @Test(timeout = 60000)
    public void testCalculateNumStreamsToRebalance() {
        String myNode = "mynode";

        // empty load distribution
        assertEquals(0, BalancerUtils.calculateNumStreamsToRebalance(
                myNode, new HashMap<String, Integer>(), 0, 10));
        // my node doesn't exist in load distribution
        Map<String, Integer> loadDistribution = new HashMap<String, Integer>();
        loadDistribution.put("node1", 10);
        assertEquals(0, BalancerUtils.calculateNumStreamsToRebalance(
                myNode, loadDistribution, 0, 10));
        // my node doesn't reach rebalance water mark
        loadDistribution.clear();
        loadDistribution.put("node1", 1);
        loadDistribution.put(myNode, 100);
        assertEquals(0, BalancerUtils.calculateNumStreamsToRebalance(
                myNode, loadDistribution, 200, 10));
        // my node is below average in the cluster.
        loadDistribution.clear();
        loadDistribution.put(myNode, 1);
        loadDistribution.put("node1", 99);
        assertEquals(0, BalancerUtils.calculateNumStreamsToRebalance(
                myNode, loadDistribution, 0, 10));
        // my node is above average in the cluster
        assertEquals(49, BalancerUtils.calculateNumStreamsToRebalance(
                "node1", loadDistribution, 0, 10));
        // my node is at the tolerance range
        loadDistribution.clear();
        loadDistribution.put(myNode, 55);
        loadDistribution.put("node1", 45);
        assertEquals(0, BalancerUtils.calculateNumStreamsToRebalance(
                myNode, loadDistribution, 0, 10));
    }
}
