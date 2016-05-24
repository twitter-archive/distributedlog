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
package com.twitter.distributedlog.client.serverset;

import com.google.common.collect.ImmutableList;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ServerSets;
import com.twitter.common.zookeeper.ZooKeeperClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * A wrapper over zookeeper client and its server set.
 */
public class DLZkServerSet {

    private static final Logger logger = LoggerFactory.getLogger(DLZkServerSet.class);

    static final String ZNODE_WRITE_PROXY = ".write_proxy";

    private static String getZKServersFromDLUri(URI uri) {
        return uri.getAuthority().replace(";", ",");
    }

    private static Iterable<InetSocketAddress> getZkAddresses(URI uri) {
        String zkServers = getZKServersFromDLUri(uri);
        String[] zkServerList = StringUtils.split(zkServers, ',');
        InetSocketAddress[] zkAddresses = new InetSocketAddress[zkServerList.length];
        int i = 0;
        for (String zkServer : zkServerList) {
            String[] hostAndPort = StringUtils.split(zkServer, ':');
            int port = 2181;
            String host = hostAndPort[0];
            if (hostAndPort.length == 2) {
                try {
                    port = Integer.parseInt(hostAndPort[1]);
                } catch (NumberFormatException nfe) {
                    logger.warn("Failed to retrieve zookeeper server port from {}. Use default port 2181.",
                            zkServer, nfe);
                }
            }
            InetSocketAddress address = InetSocketAddress.createUnresolved(host, port);
            zkAddresses[i] = address;
            ++i;
        }
        return ImmutableList.copyOf(zkAddresses);
    }

    public static DLZkServerSet of(URI uri,
                                   int zkSessionTimeoutMs) {
        // Create zookeeper and server set
        String zkPath = uri.getPath() + "/" + ZNODE_WRITE_PROXY;
        Iterable<InetSocketAddress> zkAddresses = getZkAddresses(uri);
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
