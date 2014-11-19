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
