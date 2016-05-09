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

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.ZooKeeperClient.Credentials;
import com.twitter.distributedlog.ZooKeeperClient.DigestCredentials;
import com.twitter.distributedlog.util.DLUtils;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Builder to build zookeeper client.
 */
public class ZooKeeperClientBuilder {

    static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClientBuilder.class);

    /**
     * Create a zookeeper client builder to build zookeeper clients.
     *
     * @return zookeeper client builder.
     */
    public static ZooKeeperClientBuilder newBuilder() {
        return new ZooKeeperClientBuilder();
    }

    // name
    private String name = "default";
    // sessionTimeoutMs
    private int sessionTimeoutMs = -1;
    // conectionTimeoutMs
    private int conectionTimeoutMs = -1;
    // zkServers
    private String zkServers = null;
    // retry policy
    private RetryPolicy retryPolicy = null;
    // stats logger
    private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
    // retry executor thread count
    private int retryThreadCount = 1;
    // zookeeper access requestRateLimit limit
    private double requestRateLimit = 0;
    // Did call the zkAclId setter on the builder, used to ensure the setter is set.
    private boolean zkAclIdSet = false;
    private String zkAclId;

    // Cached ZooKeeper Client
    private ZooKeeperClient cachedClient = null;

    private ZooKeeperClientBuilder() {}

    /**
     * Set zookeeper client name
     *
     * @param name zookeeper client name
     * @return zookeeper client builder
     */
    public synchronized ZooKeeperClientBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Set zookeeper session timeout in milliseconds.
     *
     * @param sessionTimeoutMs
     *          session timeout in milliseconds.
     * @return zookeeper client builder.
     */
    public synchronized ZooKeeperClientBuilder sessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        if (this.conectionTimeoutMs <= 0) {
            this.conectionTimeoutMs = 2 * sessionTimeoutMs;
        }
        return this;
    }

    public synchronized ZooKeeperClientBuilder retryThreadCount(int retryThreadCount) {
        this.retryThreadCount = retryThreadCount;
        return this;
    }

    public synchronized ZooKeeperClientBuilder requestRateLimit(double requestRateLimit) {
        this.requestRateLimit = requestRateLimit;
        return this;
    }

    /**
     * Set zookeeper connection timeout in milliseconds
     *
     * @param connectionTimeoutMs
     *          connection timeout ms.
     * @return builder
     */
    public synchronized ZooKeeperClientBuilder connectionTimeoutMs(int connectionTimeoutMs) {
        this.conectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    /**
     * Set ZooKeeper Connect String.
     *
     * @param zkServers
     *          zookeeper servers to connect.
     * @return builder
     */
    public synchronized ZooKeeperClientBuilder zkServers(String zkServers) {
        this.zkServers = zkServers;
        return this;
    }

    /**
     * Set DistributedLog URI.
     *
     * @param uri
     *          distributedlog uri.
     * @return builder.
     */
    public synchronized ZooKeeperClientBuilder uri(URI uri) {
        this.zkServers = DLUtils.getZKServersFromDLUri(uri);
        return this;
    }

    /**
     * Build zookeeper client using existing <i>zkc</i> client.
     *
     * @param zkc
     *          zookeeper client.
     * @return builder
     */
    public synchronized ZooKeeperClientBuilder zkc(ZooKeeperClient zkc) {
        this.cachedClient = zkc;
        return this;
    }

    /**
     * Build zookeeper client with given retry policy <i>retryPolicy</i>.
     *
     * @param retryPolicy
     *          retry policy
     * @return builder
     */
    public synchronized ZooKeeperClientBuilder retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    /**
     * Build zookeeper client with given stats logger <i>statsLogger</i>.
     *
     * @param statsLogger
     *          stats logger to expose zookeeper stats
     * @return builder
     */
    public synchronized ZooKeeperClientBuilder statsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        return this;
    }

    /**
     * * Build zookeeper client with given zk acl digest id <i>zkAclId</i>.
     */
    public synchronized ZooKeeperClientBuilder zkAclId(String zkAclId) {
        this.zkAclIdSet = true;
        this.zkAclId = zkAclId;
        return this;
    }

    private void validateParameters() {
        Preconditions.checkNotNull(zkServers, "No zk servers provided.");
        Preconditions.checkArgument(conectionTimeoutMs > 0,
                "Invalid connection timeout : %d", conectionTimeoutMs);
        Preconditions.checkArgument(sessionTimeoutMs > 0,
                "Invalid session timeout : %d", sessionTimeoutMs);
        Preconditions.checkNotNull(statsLogger, "No stats logger provided.");
        Preconditions.checkArgument(zkAclIdSet, "Zookeeper acl id not set.");
    }

    /**
     * Build a zookeeper client.
     *
     * @return zookeeper client.
     */
    public synchronized ZooKeeperClient build() {
        if (null == cachedClient) {
            cachedClient = buildClient();
        }
        return cachedClient;
    }

    private ZooKeeperClient buildClient() {
        validateParameters();

        Credentials credentials = Credentials.NONE;
        if (null != zkAclId) {
            credentials = new DigestCredentials(zkAclId, zkAclId);
        }

        return new ZooKeeperClient(
                name,
                sessionTimeoutMs,
                conectionTimeoutMs,
                zkServers,
                retryPolicy,
                statsLogger,
                retryThreadCount,
                requestRateLimit,
                credentials
        );
    }
}
