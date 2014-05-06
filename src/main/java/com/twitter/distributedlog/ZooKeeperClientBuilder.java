package com.twitter.distributedlog;

import com.google.common.base.Preconditions;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;

import java.net.URI;

/**
 * Builder to build zookeeper client.
 */
public class ZooKeeperClientBuilder {

    /**
     * Create a zookeeper client builder to build zookeeper clients.
     *
     * @return zookeeper client builder.
     */
    public static ZooKeeperClientBuilder newBuilder() {
        return new ZooKeeperClientBuilder();
    }

    // whether to build new client
    private boolean buildNew = false;
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


    // Cached ZooKeeper Client
    private ZooKeeperClient cachedClient = null;

    private ZooKeeperClientBuilder() {}

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
        this.zkServers = uri.getAuthority().replace(";", ",");
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
        this.buildNew = false;
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
     * If <i>buildNew</i> is set to false, the built zookeeper client by {@link #build()}
     * will be cached. Following {@link #build()} always returns this cached zookeeper
     * client. Otherwise, each {@link #build()} will create a new zookeeper client.
     *
     * @param buildNew
     *          whether to build new client for each {@link #build()}
     * @return builder
     */
    public synchronized ZooKeeperClientBuilder buildNew(boolean buildNew) {
        this.buildNew = buildNew;
        return this;
    }

    private void validateParameters() {
        Preconditions.checkNotNull(zkServers, "No zk servers provided.");
        Preconditions.checkArgument(conectionTimeoutMs > 0,
                "Invalid connection timeout : %d", conectionTimeoutMs);
        Preconditions.checkArgument(sessionTimeoutMs > 0,
                "Invalid session timeout : %d", sessionTimeoutMs);
        Preconditions.checkNotNull(statsLogger, "No stats logger provided.");
    }

    /**
     * Build a zookeeper client.
     *
     * @return zookeeper client.
     */
    public synchronized ZooKeeperClient build() {
        return build(false);
    }

    /**
     * Build a new zookeeper client.
     *
     * @return new zookeeper client.
     */
    public ZooKeeperClient buildNew() {
        return build(true);
    }

    /**
     * Build a zookeeper client. If <i>forceNew</i> is true, a new
     * ZooKeeper client is created.
     *
     * @param forceNew
     *          flag to force creating a new client.
     * @return zookeeper client.
     */
    private synchronized ZooKeeperClient build(boolean forceNew) {
        if (!buildNew && !forceNew) {
            if (null == cachedClient) {
                cachedClient = buildClient();
            } else {
                cachedClient.addRef();
            }
            return cachedClient;
        } else {
            return buildClient();
        }
    }

    private ZooKeeperClient buildClient() {
        validateParameters();
        return new ZooKeeperClient(sessionTimeoutMs, conectionTimeoutMs, zkServers, retryPolicy, statsLogger, retryThreadCount);
    }

}
