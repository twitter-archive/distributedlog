package com.twitter.distributedlog.service.announcer;

import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
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
    DLZkServerSet zkServerSet;

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
    public ServerSetAnnouncer(URI uri,
                              int servicePort,
                              int statsPort,
                              int shardId) throws UnknownHostException {
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

        // Create zookeeper and server set
        this.zkServerSet = DLZkServerSet.of(uri, 60000);
    }

    @Override
    public synchronized void announce() throws IOException {
        try {
            serviceStatus =
                    zkServerSet.getServerSet().join(serviceEndpoint, additionalEndpoints, shardId);
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
        zkServerSet.close();
    }
}
