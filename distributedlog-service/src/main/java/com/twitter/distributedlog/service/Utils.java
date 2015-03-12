package com.twitter.distributedlog.service;

import com.google.common.base.Preconditions;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common_internal.zookeeper.TwitterServerSet;
import com.twitter.common_internal.zookeeper.TwitterZk;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetSocketAddress;

/**
 * Util functions
 */
public class Utils {
    public static Iterable<InetSocketAddress> getSdZkEndpointsForDC(String dc) {
        if ("atla".equals(dc)) {
            return TwitterZk.ATLA_SD_ZK_ENDPOINTS;
        } else if ("smf1".equals(dc)) {
            return TwitterZk.SMF1_SD_ZK_ENDPOINTS;
        } else {
            return TwitterZk.SD_ZK_ENDPOINTS;
        }
    }

    public static Pair<ZooKeeperClient, ServerSet> parseServerSet(String serverSetPath) {
        String[] serverSetParts = StringUtils.split(serverSetPath, '/');
        Preconditions.checkArgument(serverSetParts.length == 3 || serverSetParts.length == 4,
                "serverset path is malformed: must be role/env/job or dc/role/env/job");
        TwitterServerSet.Service zkService;
        Iterable<InetSocketAddress> zkEndPoints;
        if (serverSetParts.length == 3) {
            zkEndPoints = TwitterZk.SD_ZK_ENDPOINTS;
            zkService = new TwitterServerSet.Service(serverSetParts[0], serverSetParts[1], serverSetParts[2]);
        } else {
            zkEndPoints = Utils.getSdZkEndpointsForDC(serverSetParts[0]);
            zkService = new TwitterServerSet.Service(serverSetParts[1], serverSetParts[2], serverSetParts[3]);
        }
        ZooKeeperClient zkClient =
                TwitterServerSet.clientBuilder(zkService).zkEndpoints(zkEndPoints).build();
        ServerSet serverSet = TwitterServerSet.create(zkClient, zkService);
        return Pair.of(zkClient, serverSet);
    }
}
