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
package com.twitter.distributedlog.service.config;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.service.streamset.IdentityStreamPartitionConverter;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SystemConfiguration;

/**
 * Configuration for DistributedLog Server
 */
public class ServerConfiguration extends CompositeConfiguration {

    private static ClassLoader defaultLoader;

    static {
        defaultLoader = Thread.currentThread().getContextClassLoader();
        if (null == defaultLoader) {
            defaultLoader = DistributedLogConfiguration.class.getClassLoader();
        }
    }

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

    // Server service timeout
    public static final String SERVER_SERVICE_TIMEOUT_MS = "server_service_timeout_ms";
    public static final String SERVER_SERVICE_TIMEOUT_MS_OLD = "serviceTimeoutMs";
    public static final long SERVER_SERVICE_TIMEOUT_MS_DEFAULT = 0;

    // Server stream probation timeout
    public static final String SERVER_STREAM_PROBATION_TIMEOUT_MS = "server_stream_probation_timeout_ms";
    public static final String SERVER_STREAM_PROBATION_TIMEOUT_MS_OLD = "streamProbationTimeoutMs";
    public static final long SERVER_STREAM_PROBATION_TIMEOUT_MS_DEFAULT = 60*1000*5;

    // Server stream to partition converter
    protected final static String SERVER_STREAM_PARTITION_CONVERTER_CLASS = "stream_partition_converter_class";

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
     * Get timeout for stream op execution in proxy layer. 0 disables timeout.
     *
     * @return timeout for stream operation in proxy layer.
     */
    public long getServiceTimeoutMs() {
        return getLong(SERVER_SERVICE_TIMEOUT_MS,
                getLong(SERVER_SERVICE_TIMEOUT_MS_OLD, SERVER_SERVICE_TIMEOUT_MS_DEFAULT));
    }

    /**
     * Set timeout for stream op execution in proxy layer. 0 disables timeout.
     *
     * @param timeoutMs
     *          timeout for stream operation in proxy layer.
     * @return dl configuration.
     */
    public ServerConfiguration setServiceTimeoutMs(long timeoutMs) {
        setProperty(SERVER_SERVICE_TIMEOUT_MS, timeoutMs);
        return this;
    }

    /**
     * After service timeout, how long should stream be kept in cache in probationary state in order
     * to prevent reacquire. In millisec.
     *
     * @return stream probation timeout in ms.
     */
    public long getStreamProbationTimeoutMs() {
        return getLong(SERVER_STREAM_PROBATION_TIMEOUT_MS,
                getLong(SERVER_STREAM_PROBATION_TIMEOUT_MS_OLD, SERVER_STREAM_PROBATION_TIMEOUT_MS_DEFAULT));
    }

    /**
     * After service timeout, how long should stream be kept in cache in probationary state in order
     * to prevent reacquire. In millisec.
     *
     * @param timeoutMs probation timeout in ms.
     */
    public ServerConfiguration setStreamProbationTimeoutMs(long timeoutMs) {
        setProperty(SERVER_STREAM_PROBATION_TIMEOUT_MS, timeoutMs);
        return this;
    }

    /**
     * Set the stream partition converter class.
     *
     * @param converterClass
     *          stream partition converter class
     * @return server configuration
     */
    public ServerConfiguration setStreamPartitionConverterClass(Class<? extends StreamPartitionConverter> converterClass) {
        setProperty(SERVER_STREAM_PARTITION_CONVERTER_CLASS, converterClass.getName());
        return this;
    }

    /**
     * Get the stream partition converter class.
     *
     * @return the stream partition converter class.
     * @throws ConfigurationException
     */
    public Class<? extends StreamPartitionConverter> getStreamPartitionConverterClass()
            throws ConfigurationException {
        return ReflectionUtils.getClass(
                this,
                SERVER_STREAM_PARTITION_CONVERTER_CLASS,
                IdentityStreamPartitionConverter.class,
                StreamPartitionConverter.class,
                defaultLoader);
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
