package com.twitter.distributedlog.service.balancer;

import java.util.Map;

/**
 * Utils for balancer.
 */
public class BalancerUtils {

    /**
     * Util function to calculate how many streams to balance for <i>nodeToRebalance</i>,
     * based on the load distribution <i>loadDistribution</i>.
     *
     * @param nodeToRebalance
     *          node to rebalance
     * @param loadDistribution
     *          load distribution map
     * @param rebalanceWaterMark
     *          if number of streams of <i>nodeToRebalance</i>
     *          is less than <i>rebalanceWaterMark</i>, no streams will be re-balanced.
     * @param tolerancePercentage
     *          tolerance percentage for the balancer. if number of streams of <i>nodeToRebalance</i>
     *          is less than average + average * <i>tolerancePercentage</i> / 100.0, no streams will
     *          be re-balanced.
     * @param <K>
     * @return number of streams to rebalance
     */
    public static <K> int calculateNumStreamsToRebalance(K nodeToRebalance,
                                                         Map<K, Integer> loadDistribution,
                                                         int rebalanceWaterMark,
                                                         double tolerancePercentage) {
        Integer myLoad = loadDistribution.get(nodeToRebalance);
        if (null == myLoad || myLoad <= rebalanceWaterMark) {
            return 0;
        }

        long totalLoad = 0L;
        int numNodes = loadDistribution.size();

        for (Map.Entry<K, Integer> entry : loadDistribution.entrySet()) {
            if (null == entry.getKey() || null == entry.getValue()) {
                continue;
            }
            totalLoad += entry.getValue();
        }

        double averageLoad = ((double) totalLoad) / numNodes;
        long permissibleLoad =
                Math.max(1L, (long) Math.ceil(averageLoad + averageLoad * tolerancePercentage / 100.0f));

        if (myLoad <= permissibleLoad) {
            return 0;
        }

        return Math.max(0, myLoad - (int) Math.ceil(averageLoad));
    }
}
