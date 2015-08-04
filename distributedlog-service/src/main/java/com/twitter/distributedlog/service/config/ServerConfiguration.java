package com.twitter.distributedlog.service.config;

import com.google.common.base.Objects;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.service.DistributedLogServiceImpl;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

/**
 * Configuration for DistributedLog Server
 */
public class ServerConfiguration extends CompositeConfiguration {

    // Server DLSN version
    protected final static String SERVER_DLSN_VERSION = "server_dlsn_version";
    protected final static byte SERVER_DLSN_VERSION_DEFAULT = DLSN.VERSION1;

    // Server mode
    protected final static String SERVER_MODE = "server_mode";
    protected final static String SERVER_MODE_DEFAULT = DistributedLogServiceImpl.ServerMode.DURABLE.toString();

    // Server latency delay
    protected final static String SERVER_LATENCY_DELAY = "server_latency_delay";
    protected final static long SERVER_LATENCY_DELAY_DEFAULT = 0L;

    // Server Region Id
    protected final static String SERVER_REGION_ID = "server_region_id";
    protected final static int SERVER_REGION_ID_DEFAULT = DistributedLogConstants.LOCAL_REGION_ID;

    // Server Port
    protected final static String SERVER_PORT = "server_port";
    protected final static int SERVER_PORT_DEFAULT = 0;

    // Server Shard Id
    protected final static String SERVER_SHARD_ID = "server_shard_id";
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
     * Get the version to encode dlsn.
     *
     * @see DLSN
     * @return version to encode dlsn.
     */
    public byte getDlsnVersion() {
        return getByte(SERVER_DLSN_VERSION, SERVER_DLSN_VERSION_DEFAULT);
    }

    /**
     * Whether the server running in durable mode
     *
     * @return true if the server running in durable mode. otherwise false.
     */
    public boolean isDurableMode() {
        return !Objects.equal(getString(SERVER_MODE), SERVER_MODE_DEFAULT);
    }

    /**
     * Get the latency delay for executing write operations.
     *
     * @return latency delay for executing write operations.
     */
    public long getLatencyDelay() {
        return getLong(SERVER_LATENCY_DELAY, SERVER_LATENCY_DELAY_DEFAULT);
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
     * Get the server port running for this service.
     *
     * @return server port
     */
    public int getServerPort() {
        return getInt(SERVER_PORT, SERVER_PORT_DEFAULT);
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
     * Whether the per stream stat enabled for not in this server.
     *
     * @return true if per stream stat enable, otherwise false.
     */
    public boolean isPerStreamStatEnabled() {
        return getBoolean(SERVER_ENABLE_PERSTREAM_STAT, SERVER_ENABLE_PERSTREAM_STAT_DEFAULT);
    }

    /**
     * Get the graceful shutdown period in millis.
     *
     * @return graceful shutdown period in millis.
     */
    public long getGracefulShutdownPeriodMs() {
        return getLong(SERVER_GRACEFUL_SHUTDOWN_PERIOD_MS, SERVER_GRACEFUL_SHUTDOWN_PERIOD_MS_DEFAULT);
    }

}
