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
package com.twitter.distributedlog;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.apache.bookkeeper.feature.FeatureProvider;

import org.apache.bookkeeper.feature.Feature;

/**
 * Builder to build bookkeeper client.
 */
public class BookKeeperClientBuilder {

    /**
     * Create a bookkeeper client builder to build bookkeeper clients.
     *
     * @return bookkeeper client builder.
     */
    public static BookKeeperClientBuilder newBuilder() {
        return new BookKeeperClientBuilder();
    }

    // client name
    private String name = null;
    // dl config
    private DistributedLogConfiguration dlConfig = null;
    // bookkeeper settings
    // zookeeper client
    private ZooKeeperClient zkc = null;
    // or zookeeper servers
    private String zkServers = null;
    // ledgers path
    private String ledgersPath = null;
    // statsLogger
    private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
    // client channel factory
    private ClientSocketChannelFactory channelFactory = null;
    // request timer
    private HashedWheelTimer requestTimer = null;
    // feature provider
    private Optional<FeatureProvider> featureProvider = Optional.absent();

    // Cached BookKeeper Client
    private BookKeeperClient cachedClient = null;

    /**
     * Private bookkeeper builder.
     */
    private BookKeeperClientBuilder() {}

    /**
     * Set client name.
     *
     * @param name
     *          client name.
     * @return builder
     */
    public synchronized BookKeeperClientBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     * <i>dlConfig</i> used to configure bookkeeper client.
     *
     * @param dlConfig
     *          distributedlog config.
     * @return builder.
     */
    public synchronized BookKeeperClientBuilder dlConfig(DistributedLogConfiguration dlConfig) {
        this.dlConfig = dlConfig;
        return this;
    }

    /**
     * Set the zkc used to build bookkeeper client. If a zookeeper client is provided in this
     * method, bookkeeper client will use it rather than creating a brand new one.
     *
     * @param zkc
     *          zookeeper client.
     * @return builder
     * @see #zkServers(String)
     */
    public synchronized BookKeeperClientBuilder zkc(ZooKeeperClient zkc) {
        this.zkc = zkc;
        return this;
    }

    /**
     * Set the zookeeper servers that bookkeeper client would connect to. If no zookeeper client
     * is provided by {@link #zkc(ZooKeeperClient)}, bookkeeper client will use the given string
     * to create a brand new zookeeper client.
     *
     * @param zkServers
     *          zookeeper servers that bookkeeper client would connect to.
     * @return builder
     * @see #zkc(ZooKeeperClient)
     */
    public synchronized BookKeeperClientBuilder zkServers(String zkServers) {
        this.zkServers = zkServers;
        return this;
    }

    /**
     * Set the ledgers path that bookkeeper client is going to access.
     *
     * @param ledgersPath
     *          ledgers path
     * @return builder
     * @see org.apache.bookkeeper.conf.ClientConfiguration#getZkLedgersRootPath()
     */
    public synchronized BookKeeperClientBuilder ledgersPath(String ledgersPath) {
        this.ledgersPath = ledgersPath;
        return this;
    }

    /**
     * Build BookKeeper client using existing <i>bkc</i> client.
     *
     * @param bkc
     *          bookkeeper client.
     * @return builder
     */
    public synchronized BookKeeperClientBuilder bkc(BookKeeperClient bkc) {
        this.cachedClient = bkc;
        return this;
    }

    /**
     * Build BookKeeper client using existing <i>channelFactory</i>.
     *
     * @param channelFactory
     *          Channel Factory used to build bookkeeper client.
     * @return bookkeeper client builder.
     */
    public synchronized BookKeeperClientBuilder channelFactory(ClientSocketChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
        return this;
    }

    /**
     * Build BookKeeper client using existing <i>request timer</i>.
     *
     * @param requestTimer
     *          HashedWheelTimer used to build bookkeeper client.
     * @return bookkeeper client builder.
     */
    public synchronized BookKeeperClientBuilder requestTimer(HashedWheelTimer requestTimer) {
        this.requestTimer = requestTimer;
        return this;
    }

    /**
     * Build BookKeeper Client using given stats logger <i>statsLogger</i>.
     *
     * @param statsLogger
     *          stats logger to report stats
     * @return builder.
     */
    public synchronized BookKeeperClientBuilder statsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        return this;
    }

    public synchronized BookKeeperClientBuilder featureProvider(Optional<FeatureProvider> featureProvider) {
        this.featureProvider = featureProvider;
        return this;
    }

    private void validateParameters() {
        Preconditions.checkNotNull(name, "Missing client name.");
        Preconditions.checkNotNull(dlConfig, "Missing DistributedLog Configuration.");
        Preconditions.checkArgument(null == zkc || null == zkServers, "Missing zookeeper setting.");
        Preconditions.checkNotNull(ledgersPath, "Missing Ledgers Root Path.");
    }

    public synchronized BookKeeperClient build() {
        if (null == cachedClient) {
            cachedClient = buildClient();
        }
        return cachedClient;
    }

    private BookKeeperClient buildClient() {
        validateParameters();
        return new BookKeeperClient(dlConfig, name, zkServers, zkc, ledgersPath, channelFactory, requestTimer, statsLogger, featureProvider);
    }
}
