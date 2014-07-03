package com.twitter.distributedlog.service.announcer;

import com.google.common.base.Preconditions;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common_internal.zookeeper.TwitterServerSet;
import com.twitter.common_internal.zookeeper.TwitterZk;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class ServerSetAnnouncer implements Announcer {

    static final Logger logger = LoggerFactory.getLogger(ServerSetAnnouncer.class);

    final String localAddr;
    final InetSocketAddress serviceEndpoint;
    final Map<String, InetSocketAddress> additionalEndpoints;
    final int shardId;

    // ServerSet
    final ServerSet serverSet;
    final ZooKeeperClient zkClient;

    // Service Status
    ServerSet.EndpointStatus serviceStatus = null;

    /**
     * Announce server infos.
     *
     * @param servicePort
     *          service port
     * @param statsPort
     *          stats port
     * @param shardId
     *          shard id
     */
    public ServerSetAnnouncer(String serverSetPath,
                              int servicePort,
                              int statsPort,
                              int shardId) throws UnknownHostException {
        String[] serverSetPathParts = StringUtils.split(serverSetPath, '/');
        Preconditions.checkArgument(serverSetPathParts.length == 3,
                "Invalid serverset path : " + serverSetPath);

        this.shardId = shardId;
        this.localAddr = InetAddress.getLocalHost().getHostAddress();
        // service endpoint
        this.serviceEndpoint = new InetSocketAddress(localAddr, servicePort);
        // stats endpoint
        InetSocketAddress statsEndpoint = new InetSocketAddress(localAddr, statsPort);
        this.additionalEndpoints = new HashMap<String, InetSocketAddress>();
        this.additionalEndpoints.put("aurora", statsEndpoint);
        this.additionalEndpoints.put("stats", statsEndpoint);
        this.additionalEndpoints.put("service", serviceEndpoint);
        this.additionalEndpoints.put("thrift", serviceEndpoint);

        // Server Set Client
        TwitterServerSet.Service zkService = new TwitterServerSet.Service(
                serverSetPathParts[0], serverSetPathParts[1], serverSetPathParts[2]);
        zkClient = TwitterServerSet
                .clientBuilder(zkService)
                .zkEndpoints(TwitterZk.SD_ZK_ENDPOINTS)
                .build();
        serverSet = TwitterServerSet.create(zkClient, zkService);
    }

    @Override
    public synchronized void announce() throws IOException {
        try {
            serviceStatus =
                    serverSet.join(serviceEndpoint, additionalEndpoints, shardId);
        } catch (Group.JoinException e) {
            throw new IOException("Failed to announce service : ", e);
        } catch (InterruptedException e) {
            logger.warn("Interrupted on announcing service : ", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized void unannounce() throws IOException {
        if (null == serviceStatus) {
            logger.warn("No service to unannounce.");
            return;
        }
        try {
            serviceStatus.leave();
        } catch (ServerSet.UpdateException e) {
            throw new IOException("Failed to unannounce service : ", e);
        }
    }

    @Override
    public void close() {
        zkClient.close();
    }
}
