package com.twitter.distributedlog.client.serverset;

import com.google.common.collect.ImmutableList;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ServerSets;
import com.twitter.common.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooDefs;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * A wrapper over zookeeper client and its server set.
 */
public class DLZkServerSet {

    static final String ZNODE_WRITE_PROXY = ".write_proxy";

    private static String getZKServersFromDLUri(URI uri) {
        return uri.getAuthority().replace(";", ",");
    }

    public static DLZkServerSet of(URI uri,
                                   int zkSessionTimeoutMs) {
        // Create zookeeper and server set
        String zkServers = getZKServersFromDLUri(uri);
        String zkPath = uri.getPath() + "/" + ZNODE_WRITE_PROXY;
        Iterable<InetSocketAddress> zkAddresses =
                ImmutableList.of(InetSocketAddress.createUnresolved(zkServers, 2181));
        ZooKeeperClient zkClient =
                new ZooKeeperClient(Amount.of(zkSessionTimeoutMs, Time.MILLISECONDS), zkAddresses);
        ServerSet serverSet = ServerSets.create(zkClient, ZooDefs.Ids.OPEN_ACL_UNSAFE, zkPath);
        return new DLZkServerSet(zkClient, serverSet);
    }

    private final ZooKeeperClient zkClient;
    private final ServerSet zkServerSet;

    public DLZkServerSet(ZooKeeperClient zkClient,
                         ServerSet zkServerSet) {
        this.zkClient = zkClient;
        this.zkServerSet = zkServerSet;
    }

    public ZooKeeperClient getZkClient() {
        return zkClient;
    }

    public ServerSet getServerSet() {
        return zkServerSet;
    }

    public void close() {
        zkClient.close();
    }
}
