package com.twitter.distributedlog.service.config;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

/**
 * Configuration for DistributedLog Server
 */
public class ServerConfiguration extends CompositeConfiguration {

    // Server DLSN version
    protected final static String SERVER_DLSN_VERSION = "server_dlsn_version";
    protected final static byte SERVER_DLSN_VERSION_DEFAULT = DLSN.VERSION1;

    // Server Durable Write Enable/Disable Flag
    protected final static String SERVER_DURABLE_WRITE_ENABLED = "server_durable_write_enabled";
    protected final static boolean SERVER_DURABLE_WRITE_ENABLED_DEFAULT = true;

    // Server Region Id
    protected final static String SERVER_REGION_ID = "server_region_id";
    protected final static int SERVER_REGION_ID_DEFAULT = DistributedLogConstants.LOCAL_REGION_ID;

    // Server Port
    protected final static String SERVER_PORT = "server_port";
    protected final static int SERVER_PORT_DEFAULT = 0;

    // Server Shard Id
    protected final static String SERVER_SHARD_ID = "server_shard";
    protected final static int SERVER_SHARD_ID_DEFAULT = -1;

    // Server Threads
    protected final static String SERVER_NUM_THREADS = "server_threads";
    protected final static int SERVER_NUM_THREADS_DEFAULT = Runtime.getRuntime().availableProcessors();

    // Server enable per stream stat
    protected final static String SERVER_ENABLE_PERSTREAM_STAT = "server_enable_perstream_stat";
    protected final static boolean SERVER_ENABLE_PERSTREAM_STAT_DEFAULT = true;

    // Server graceful shutdown period (in millis)
    protected final static String SERVER_GRACEFUL_SHUTDOWN_PERIOD_MS = "server_graceful_shutdown_period_ms";
    protected final static long SERVER_GRACEFUL_SHUTDOWN_PERIOD_MS_DEFAULT = 0L;

    public ServerConfiguration() {
        super();
        addConfiguration(new SystemConfiguration());
    }

    /**
     * Load configurations from {@link DistributedLogConfiguration}
     *
     * @param dlConf
     *          distributedlog configuration
     */
    public void loadConf(DistributedLogConfiguration dlConf) {
        addConfiguration(dlConf);
    }

    /**
     * Set the version to encode dlsn.
     *
     * @param version
     *          dlsn version
     * @return server configuration
     */
    public ServerConfiguration setDlsnVersion(byte version) {
        setProperty(SERVER_DLSN_VERSION, version);
        return this;
    }

    /**
     * Get the version to encode dlsn.
     *
     * @see DLSN
     * @return version to encode dlsn.
     */
    public byte getDlsnVersion() {
        return getByte(SERVER_DLSN_VERSION, SERVER_DLSN_VERSION_DEFAULT);
    }

    /**
     * Set the flag to enable/disable durable write
     *
     * @param enabled
     *          flag to enable/disable durable write
     * @return server configuration
     */
    public ServerConfiguration enableDurableWrite(boolean enabled) {
        setProperty(SERVER_DURABLE_WRITE_ENABLED, enabled);
        return this;
    }

    /**
     * Is durable write enabled?
     *
     * @return true if waiting writes to be durable. otherwise false. 
     */
    public boolean isDurableWriteEnabled() {
        return getBoolean(SERVER_DURABLE_WRITE_ENABLED, SERVER_DURABLE_WRITE_ENABLED_DEFAULT);
    }

    /**
     * Set the region id used to instantiate DistributedLogNamespace
     *
     * @param regionId
     *          region id
     * @return server configuration
     */
    public ServerConfiguration setRegionId(int regionId) {
        setProperty(SERVER_REGION_ID, regionId);
        return this;
    }

    /**
     * Get the region id used to instantiate
     * {@link com.twitter.distributedlog.namespace.DistributedLogNamespace}
     *
     * @return region id used to instantiate DistributedLogNamespace
     */
    public int getRegionId() {
        return getInt(SERVER_REGION_ID, SERVER_REGION_ID_DEFAULT);
    }

    /**
     * Set the server port running for this service.
     *
     * @param port
     *          server port
     * @return server configuration
     */
    public ServerConfiguration setServerPort(int port) {
        setProperty(SERVER_PORT, port);
        return this;
    }

    /**
     * Get the server port running for this service.
     *
     * @return server port
     */
    public int getServerPort() {
        return getInt(SERVER_PORT, SERVER_PORT_DEFAULT);
    }

    /**
     * Set the shard id of this server.
     *
     * @param shardId
     *          shard id
     * @return shard id of this server
     */
    public ServerConfiguration setServerShardId(int shardId) {
        setProperty(SERVER_SHARD_ID, shardId);
        return this;
    }

    /**
     * Get the shard id of this server. It would be used to instantiate the client id
     * used for DistributedLogNamespace.
     *
     * @return shard id of this server.
     */
    public int getServerShardId() {
        return getInt(SERVER_SHARD_ID, SERVER_SHARD_ID_DEFAULT);
    }

    /**
     * Get the number of threads for the executor of this server.
     *
     * @return number of threads for the executor running in this server.
     */
    public int getServerThreads() {
        return getInt(SERVER_NUM_THREADS, SERVER_NUM_THREADS_DEFAULT);
    }

    /**
     * Set the number of threads for the executor of this server.
     *
     * @param numThreads
     *          number of threads for the executor running in this server.
     * @return server configuration
     */
    public ServerConfiguration setServerThreads(int numThreads) {
        setProperty(SERVER_NUM_THREADS, numThreads);
        return this;
    }

    /**
     * Enable/Disable per stream stat.
     *
     * @param enabled
     *          flag to enable/disable per stream stat
     * @return server configuration
     */
    public ServerConfiguration setPerStreamStatEnabled(boolean enabled) {
        setProperty(SERVER_ENABLE_PERSTREAM_STAT, enabled);
        return this;
    }

    /**
     * Whether the per stream stat enabled for not in this server.
     *
     * @return true if per stream stat enable, otherwise false.
     */
    public boolean isPerStreamStatEnabled() {
        return getBoolean(SERVER_ENABLE_PERSTREAM_STAT, SERVER_ENABLE_PERSTREAM_STAT_DEFAULT);
    }

    /**
     * Set the graceful shutdown period in millis.
     *
     * @param periodMs
     *          graceful shutdown period in millis.
     * @return server configuration
     */
    public ServerConfiguration setGracefulShutdownPeriodMs(long periodMs) {
        setProperty(SERVER_GRACEFUL_SHUTDOWN_PERIOD_MS, periodMs);
        return this;
    }

    /**
     * Get the graceful shutdown period in millis.
     *
     * @return graceful shutdown period in millis.
     */
    public long getGracefulShutdownPeriodMs() {
        return getLong(SERVER_GRACEFUL_SHUTDOWN_PERIOD_MS, SERVER_GRACEFUL_SHUTDOWN_PERIOD_MS_DEFAULT);
    }

    /**
     * Validate the configuration
     */
    public void validate() {
        byte dlsnVersion = getDlsnVersion();
        Preconditions.checkArgument(dlsnVersion >= DLSN.VERSION0 && dlsnVersion <= DLSN.VERSION1,
                "Unknown dlsn version " + dlsnVersion);
        Preconditions.checkArgument(getServerThreads() > 0,
                "Invalid number of server threads : " + getServerThreads());
        Preconditions.checkArgument(getServerShardId() >= 0,
                "Invalid server shard id : " + getServerShardId());
    }

}
