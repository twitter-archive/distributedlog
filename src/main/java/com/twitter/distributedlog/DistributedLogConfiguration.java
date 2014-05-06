package com.twitter.distributedlog;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class DistributedLogConfiguration extends CompositeConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogConfiguration.class);

    private static ClassLoader defaultLoader;

    static {
        defaultLoader = Thread.currentThread().getContextClassLoader();
        if (null == defaultLoader) {
            defaultLoader = DistributedLogConfiguration.class.getClassLoader();
        }
    }

    // Name for the default (non-partitioned) stream
    public static final String BKDL_UNPARTITIONED_STREAM_NAME = "unpartitionedStreamName";
    public static final String BKDL_UNPARTITIONED_STREAM_NAME_DEFAULT = "<default>";

    // Controls when log records accumulated in the writer will be
    // transmitted to bookkeeper
    public static final String BKDL_OUTPUT_BUFFER_SIZE = "output-buffer-size";
    public static final int BKDL_OUTPUT_BUFFER_SIZE_DEFAULT = 1024;

    public static final String BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS = "periodicFlushFrequencyMilliSeconds";
    public static final int BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT = 0;

    // Controls the retention period after which old ledgers are deleted
    public static final String BKDL_RETENTION_PERIOD_IN_HOURS = "retention-size";
    public static final int BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT = 72;

    // The time after which the a given log stream switches to a new ledger
    public static final String BKDL_ROLLING_INTERVAL_IN_MINUTES = "rolling-interval";
    public static final int BKDL_ROLLING_INTERVAL_IN_MINUTES_DEFAULT = 120;

    // Bookkeeper ensemble size
    public static final String BKDL_BOOKKEEPER_ENSEMBLE_SIZE = "ensemble-size";
    public static final int BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT = 3;

    // Bookkeeper quorum size
    public static final String BKDL_BOOKKEEPER_QUORUM_SIZE = "quorum-size";
    public static final int BKDL_BOOKKEEPER_QUORUM_SIZE_DEFAULT = 2;

    // Bookkeeper write quorum size
    public static final String BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE = "write-quorum-size";
    public static final int BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT = 2;

    // Bookkeeper ack quorum size
    public static final String BKDL_BOOKKEEPER_ACK_QUORUM_SIZE = "ack-quorum-size";
    public static final int BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT = 2;

    // Bookkeeper digest
    public static final String BKDL_BOOKKEEPER_DIGEST_PW = "digestPw";
    public static final String BKDL_BOOKKEEPER_DIGEST_PW_DEFAULT = "";

    public static final String BKDL_BOOKKEEPER_LEDGERS_PATH = "bkLedgersPath";
    public static final String BKDL_BOOKKEEPER_LEDGERS_PATH_DEFAULT = "/ledgers";

    // Executor Parameters
    public static final String BKDL_NUM_WORKER_THREADS = "numWorkerThreads";

    // Reader parameters
    public static final String BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS = "readerIdleWarnThresholdMillis";
    public static final int BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS_DEFAULT = 120000;


    // Read ahead related parameters
    public static final String BKDL_ENABLE_READAHEAD = "enableReadAhead";
    public static final boolean BKDL_ENABLE_READAHEAD_DEFAULT = true;

    public static final String BKDL_READAHEAD_MAX_ENTRIES = "ReadAheadMaxEntries";
    public static final int BKDL_READAHEAD_MAX_ENTRIES_DEFAULT = 10;

    public static final String BKDL_READAHEAD_BATCHSIZE = "ReadAheadBatchSize";
    public static final int BKDL_READAHEAD_BATCHSIZE_DEFAULT = 2;

    public static final String BKDL_READAHEAD_WAITTIME = "ReadAheadWaitTime";
    public static final int BKDL_READAHEAD_WAITTIME_DEFAULT = 200;

    // should each partition use a separate zookeeper client
    public static final String BKDL_SEPARATE_ZK_CLIENT = "separateZKClients";
    public static final boolean BKDL_SEPARATE_ZK_CLIENT_DEFAULT = false;

    public static final String BKDL_ZK_SESSION_TIMEOUT_SECONDS = "zkSessionTimeoutSeconds";
    public static final int BKDL_ZK_SESSION_TIMEOUT_SECONDS_DEFAULT = 30;

    public static final String BKDL_ZK_NUM_RETRIES = "zkNumRetries";
    public static final int BKDL_ZK_NUM_RETRIES_DEFAULT = 3;

    public static final String BKDL_ZK_RETRY_BACKOFF_START_MILLIS = "zkRetryStartBackoffMillis";
    public static final int BKDL_ZK_RETRY_BACKOFF_START_MILLIS_DEFAULT = 5000;

    public static final String BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS = "zkRetryMaxBackoffMillis";
    public static final int BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS_DEFAULT = 30000;

    public static final String BKDL_ZK_PREFIX = "dlZKPrefix";
    public static final String BKDL_ZK_PREFIX_DEFAULT = "/messaging/distributedlog";

    public static final String BKDL_SANITYCHECK_BEFORE_DELETE = "sanityCheckDelete";
    public static final boolean BKDL_SANITYCHECK_BEFORE_DELETE_DEFAULT = true;

    public static final String BKDL_ENABLE_RECORD_COUNTS = "enableRecordCounts";
    public static final boolean BKDL_ENABLE_RECORD_COUNTS_DEFAULT = true;

    // Various timeouts - names are self explanatory
    public static final String BKDL_LOG_FLUSH_TIMEOUT = "logFlushTimeoutSeconds";
    public static final int BKDL_LOG_FLUSH_TIMEOUT_DEFAULT = 30;

    public static final String BKDL_LOCK_TIMEOUT = "lockTimeoutSeconds";
    public static final long BKDL_LOCK_TIMEOUT_DEFAULT = 30;

    public static final String BKDL_BKCLIENT_ZK_SESSION_TIMEOUT = "bkcZKSessionTimeoutSeconds";
    public static final int BKDL_BKCLIENT_ZK_SESSION_TIMEOUT_DEFAULT = 30;

    public static final String BKDL_BKCLIENT_READ_TIMEOUT = "bkcReadTimeoutSeconds";
    public static final int BKDL_BKCLIENT_READ_TIMEOUT_DEFAULT = 10;

    public static final String BKDL_BKCLIENT_WRITE_TIMEOUT = "bkcWriteTimeoutSeconds";
    public static final int BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT = 10;

    public static final String BKDL_BKCLIENT_NUM_WORKER_THREADS = "bkcNumWorkerThreads";
    public static final int BKDL_BKCLEINT_NUM_WORKER_THREADS_DEFAULT = 1;

    public static final String BKDL_BKCLIENT_NUM_IO_THREADS = "bkcNumIOThreads";

    public static final String BKDL_ZKCLIENT_NUM_RETRY_THREADS = "zkcNumRetryThreads";
    public static final int BKDL_ZKCLIENT_NUM_RETRY_THREADS_DEFAULT = 1;

    public static final String BKDL_TIMEOUT_TIMER_TICK_DURATION_MS = "timerTickDuration";
    public static final long BKDL_TIMEOUT_TIMER_TICK_DURATION_MS_DEFAULT = 100;

    public static final String BKDL_TIMEOUT_TIMER_NUM_TICKS = "timerNumTicks";
    public static final int BKDL_TIMEOUT_TIMER_NUM_TICKS_DEFAULT = 1024;

    public DistributedLogConfiguration() {
        super();
        // add configuration for system properties
        addConfiguration(new SystemConfiguration());
    }

    /**
     * You can load configurations in precedence order. The first one takes
     * precedence over any loaded later.
     *
     * @param confURL Configuration URL
     */
    public void loadConf(URL confURL) throws ConfigurationException {
        Configuration loadedConf = new PropertiesConfiguration(confURL);
        addConfiguration(loadedConf);
    }

    /**
     * You can load configuration from other configuration
     *
     * @param baseConf Other Configuration
     */
    public void loadConf(DistributedLogConfiguration baseConf) {
        addConfiguration(baseConf);
    }

    /**
     * Load configuration from other configuration object
     *
     * @param otherConf Other configuration object
     */
    public void loadConf(Configuration otherConf) {
        addConfiguration(otherConf);
    }

    /**
     * Get name of the unpartitioned stream
     *
     * @return unpartitioned stream
     */
    public String getUnpartitionedStreamName() {
        return getString(BKDL_UNPARTITIONED_STREAM_NAME, BKDL_UNPARTITIONED_STREAM_NAME_DEFAULT);
    }

    /**
     * Set name of the unpartitioned stream
     *
     * @param streamName name of the unpartitioned stream
     */
    public void setUnpartitionedStreamName(String streamName) {
        setProperty(BKDL_UNPARTITIONED_STREAM_NAME, streamName);
    }

    /**
     * Get retention period in hours
     *
     * @return retention period in hours
     */
    public int getRetentionPeriodHours() {
        return this.getInt(BKDL_RETENTION_PERIOD_IN_HOURS, BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT);
    }

    /**
     * Set retention period in hours
     *
     * @param retentionHours retention period in hours.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setRetentionPeriodHours(int retentionHours) {
        setProperty(BKDL_RETENTION_PERIOD_IN_HOURS, retentionHours);
        return this;
    }

    /**
     * Get rolling interval in minutes
     *
     * @return buffer size
     */
    public int getLogSegmentRollingIntervalMinutes() {
        return this.getInt(BKDL_ROLLING_INTERVAL_IN_MINUTES, BKDL_ROLLING_INTERVAL_IN_MINUTES_DEFAULT);
    }

    /**
     * Set rolling interval in minutes.
     *
     * @param rollingMinutes rolling interval in minutes.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setLogSegmentRollingIntervalMinutes(int rollingMinutes) {
        setProperty(BKDL_ROLLING_INTERVAL_IN_MINUTES, rollingMinutes);
        return this;
    }

    /**
     * Get output buffer size
     *
     * @return buffer size
     */
    public int getOutputBufferSize() {
        return this.getInt(BKDL_OUTPUT_BUFFER_SIZE, BKDL_OUTPUT_BUFFER_SIZE_DEFAULT);
    }

    /**
     * Set output buffer size.
     *
     * @param opBufferSize output buffer size.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setOutputBufferSize(int opBufferSize) {
        setProperty(BKDL_OUTPUT_BUFFER_SIZE, opBufferSize);
        return this;
    }

    /**
     * Get ensemble size
     *
     * @return ensemble size
     */
    public int getEnsembleSize() {
        return this.getInt(BKDL_BOOKKEEPER_ENSEMBLE_SIZE, BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT);
    }

    /**
     * Set ensemble size.
     *
     * @param ensembleSize ensemble size.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setEnsembleSize(int ensembleSize) {
        setProperty(BKDL_BOOKKEEPER_ENSEMBLE_SIZE, ensembleSize);
        return this;
    }

    /**
     * Get quorum size
     *
     * @return quorum size
     */
    @Deprecated
    public int getQuorumSize() {
        return this.getInt(BKDL_BOOKKEEPER_QUORUM_SIZE, BKDL_BOOKKEEPER_QUORUM_SIZE_DEFAULT);
    }

    /**
     * Set quorum size.
     *
     * @param quorumSize quorum size.
     * @return distributed log configuration
     */
    @Deprecated
    public DistributedLogConfiguration setQuorumSize(int quorumSize) {
        setProperty(BKDL_BOOKKEEPER_QUORUM_SIZE, quorumSize);
        return this;
    }

    /**
     * Get write quorum size.
     *
     * @return write quorum size
     */
    public int getWriteQuorumSize() {
        return this.getInt(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE, BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT);
    }

    /**
     * Set write quorum size.
     *
     * @param quorumSize
     *          quorum size.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setWriteQuorumSize(int quorumSize) {
        setProperty(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE, quorumSize);
        return this;
    }

    /**
     * Get ack quorum size.
     *
     * @return ack quorum size
     */
    public int getAckQuorumSize() {
        return this.getInt(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE, BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT);
    }

    /**
     * Set ack quorum size.
     *
     * @param quorumSize
     *          quorum size.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setAckQuorumSize(int quorumSize) {
        setProperty(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE, quorumSize);
        return this;
    }

    /**
     * Set BK password digest
     *
     * @param bkDigestPW BK password digest
     */
    public void setBKDigestPW(String bkDigestPW) {
        setProperty(BKDL_BOOKKEEPER_DIGEST_PW, bkDigestPW);
    }

    /**
     * Get BK password digest.
     *
     * @return zk ledgers root path
     */
    public String getBKDigestPW() {
        return getString(BKDL_BOOKKEEPER_DIGEST_PW, BKDL_BOOKKEEPER_DIGEST_PW_DEFAULT);
    }

    /**
     * Set if we should use separate ZK clients
     *
     * @param separateZKClients
     *          Use separate ZK Clients
     */
    public DistributedLogConfiguration setSeparateZKClients(boolean separateZKClients) {
        setProperty(BKDL_SEPARATE_ZK_CLIENT, separateZKClients);
        return this;
    }

    /**
     * Get if we should use separate ZK Clients
     *
     * @return if should use separate ZK Clients
     */
    public boolean getSeparateZKClients() {
        return getBoolean(BKDL_SEPARATE_ZK_CLIENT, BKDL_SEPARATE_ZK_CLIENT_DEFAULT);
    }

    /**
     * Set the number of worker threads used by distributedlog manager factory.
     *
     * @param numWorkerThreads
     *          number of worker threads used by distributedlog manager factory.
     * @return configuration
     */
    public DistributedLogConfiguration setNumWorkerThreads(int numWorkerThreads) {
        setProperty(BKDL_NUM_WORKER_THREADS, numWorkerThreads);
        return this;
    }

    /**
     * Get the number of worker threads used by distributedlog manager factory.
     *
     * @return number of worker threads used by distributedlog manager factory.
     */
    public int getNumWorkerThreads() {
        return getInt(BKDL_NUM_WORKER_THREADS, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Get BK client number of i/o threads.
     *
     * @return number of bookkeeper netty i/o threads.
     */
    public int getBKClientNumberIOThreads() {
        return this.getInt(BKDL_BKCLIENT_NUM_IO_THREADS, getNumWorkerThreads());
    }

    /**
     * Set BK client number of i/o threads.
     *
     * @param numThreads
     *          number io threads.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setBKClientNumberIOThreads(int numThreads) {
        setProperty(BKDL_BKCLIENT_NUM_IO_THREADS, numThreads);
        return this;
    }

    /**
     * Set if we should enable read ahead
     *
     * @param enableReadAhead
     *          Enable read ahead
     */
    public DistributedLogConfiguration setEnableReadAhead(boolean enableReadAhead) {
        setProperty(BKDL_ENABLE_READAHEAD, enableReadAhead);
        return this;
    }

    /**
     * Get if we should use separate ZK Clients
     *
     * @return if should use separate ZK Clients
     */
    public boolean getEnableReadAhead() {
        return getBoolean(BKDL_ENABLE_READAHEAD, BKDL_ENABLE_READAHEAD_DEFAULT);
    }

    /**
     * Get ZK Session timeout
     *
     * @return ensemble size
     */
    public int getZKSessionTimeoutSeconds() {
        return this.getInt(BKDL_ZK_SESSION_TIMEOUT_SECONDS, BKDL_ZK_SESSION_TIMEOUT_SECONDS_DEFAULT);
    }

    /**
     * Get ZK Session timeout in milliseconds.
     *
     * @return zk session timeout in milliseconds.
     */
    public int getZKSessionTimeoutMilliseconds() {
        return this.getInt(BKDL_ZK_SESSION_TIMEOUT_SECONDS, BKDL_ZK_SESSION_TIMEOUT_SECONDS_DEFAULT) * 1000;
    }

    /**
     * Set ZK Session Timeout.
     *
     * @param zkSessionTimeoutSeconds session timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setZKSessionTimeoutSeconds(int zkSessionTimeoutSeconds) {
        setProperty(BKDL_ZK_SESSION_TIMEOUT_SECONDS, zkSessionTimeoutSeconds);
        return this;
    }

    /**
     * Get num of retries for zookeeper client.
     *
     * @return num of retries of zookeeper client.
     */
    public int getZKNumRetries() {
        return this.getInt(BKDL_ZK_NUM_RETRIES, BKDL_ZK_NUM_RETRIES_DEFAULT);
    }

    /**
     * Get the start backoff time of zookeeper operation retries, in seconds.
     *
     * @return start backoff time of zookeeper operation retries.
     */
    public int getZKRetryBackoffStartMillis() {
        return this.getInt(BKDL_ZK_RETRY_BACKOFF_START_MILLIS,
            BKDL_ZK_RETRY_BACKOFF_START_MILLIS_DEFAULT);
    }

    /**
     * Get the max backoff time of zookeeper operation retries, in seconds.
     *
     * @return max backoff time of zookeeper operation retries.
     */
    public int getZKRetryBackoffMaxMillis() {
        return this.getInt(BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS,
            BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS_DEFAULT);
    }

    /**
     * Set num of retries for zookeeper client.
     *
     * @param zkNumRetries num of retries of zookeeper client.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setZKNumRetries(int zkNumRetries) {
        setProperty(BKDL_ZK_NUM_RETRIES, zkNumRetries);
        return this;
    }

    /**
     * Set the start backoff time of zookeeper operation retries, in seconds.
     *
     * @param zkRetryBackoffStartMillis start backoff time of zookeeper operation retries.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setZKRetryBackoffStartMillis(int zkRetryBackoffStartMillis) {
        setProperty(BKDL_ZK_RETRY_BACKOFF_START_MILLIS, zkRetryBackoffStartMillis);
        return this;
    }

    /**
     * Set the max backoff time of zookeeper operation retries, in seconds.
     *
     * @param zkRetryBackoffMaxMillis max backoff time of zookeeper operation retries.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setZKRetryBackoffMaxMillis(int zkRetryBackoffMaxMillis) {
        setProperty(BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS, zkRetryBackoffMaxMillis);
        return this;
    }


    /**
     * Get Log Flush timeout
     *
     * @return ensemble size
     */
    public int getLogFlushTimeoutSeconds() {
        return this.getInt(BKDL_LOG_FLUSH_TIMEOUT, BKDL_LOG_FLUSH_TIMEOUT_DEFAULT);
    }

    /**
     * Set Log Flush Timeout.
     *
     * @param logFlushTimeoutSeconds log flush timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setLogFlushTimeoutSeconds(int logFlushTimeoutSeconds) {
        setProperty(BKDL_LOG_FLUSH_TIMEOUT, logFlushTimeoutSeconds);
        return this;
    }

    /**
     * Get Periodic Log Flush Frequency in seconds
     *
     * @return ensemble size
     */
    public int getPeriodicFlushFrequencyMilliSeconds() {
        return this.getInt(BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS, BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT);
    }

    /**
     * Set Periodic Log Flush Frequency in seconds.
     *
     * @param flushFrequencyMs periodic flush frequency in milliseconds.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setPeriodicFlushFrequencyMilliSeconds(int flushFrequencyMs) {
        setProperty(BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS, flushFrequencyMs);
        return this;
    }

    /**
     * Get ZK Session timeout
     *
     * @return ensemble size
     */
    public int getReadAheadBatchSize() {
        return this.getInt(BKDL_READAHEAD_BATCHSIZE, BKDL_READAHEAD_BATCHSIZE_DEFAULT);
    }

    /**
     * Set Read Ahead Batch Size.
     *
     * @param readAheadBatchSize
     *          Read ahead batch size.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setReadAheadBatchSize(int readAheadBatchSize) {
        setProperty(BKDL_READAHEAD_BATCHSIZE, readAheadBatchSize);
        return this;
    }

    /**
     * Get ZK Session timeout
     *
     * @return ensemble size
     */
    public int getReadAheadWaitTime() {
        return this.getInt(BKDL_READAHEAD_WAITTIME, BKDL_READAHEAD_WAITTIME_DEFAULT);
    }

    /**
     * Set the wait time between successive attempts to check for new log records
     *
     * @param readAheadWaitTime read ahead wait time
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setReadAheadWaitTime(int readAheadWaitTime) {
        setProperty(BKDL_READAHEAD_WAITTIME, readAheadWaitTime);
        return this;
    }

    /**
     * Get ZK Session timeout
     *
     * @return ensemble size
     */
    public int getReadAheadMaxEntries() {
        return this.getInt(BKDL_READAHEAD_MAX_ENTRIES, BKDL_READAHEAD_MAX_ENTRIES_DEFAULT);
    }

    /**
     * Set the maximum outstanding read ahead entries
     *
     * @param readAheadMaxEntries session timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setReadAheadMaxEntries(int readAheadMaxEntries) {
        setProperty(BKDL_READAHEAD_MAX_ENTRIES, readAheadMaxEntries);
        return this;
    }

    /**
     * Set BK Ledgers path
     *
     * @deprecated BookKeeper configuration is self-managed by DL. You should not rely on this.
     * @param bkLedgersPath
     *          BK ledgers path
     */
    @Deprecated
    public DistributedLogConfiguration setBKLedgersPath(String bkLedgersPath) {
        setProperty(BKDL_BOOKKEEPER_LEDGERS_PATH, bkLedgersPath);
        return this;
    }

    /**
     * Get BK ledgers path.
     *
     * @deprecated BookKeeper configuration is self-managed by DL. You should not rely on this.
     * @return bk ledgers root path
     */
    @Deprecated
    public String getBKLedgersPath() {
        return getString(BKDL_BOOKKEEPER_LEDGERS_PATH, BKDL_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
    }

    /**
     * Set path prefix for the distributedlog path in ZK
     *
     * @deprecated The DL zk prefix is self-explained in the URI. You should not rely on this.
     * @param dlZKPath
     *          distributedlog ZK path
     */
    @Deprecated
    public DistributedLogConfiguration setDLZKPathPrefix(String dlZKPath) {
        setProperty(BKDL_ZK_PREFIX, dlZKPath);
        return this;
    }

    /**
     * Get path prefix for the distributedlog path in ZK
     *
     * @deprecated The DL zk prefix is self-explained in the URI. You should not rely on this.
     * @return bk ledgers root path
     */
    @Deprecated
    public String getDLZKPathPrefix() {
        return getString(BKDL_ZK_PREFIX, BKDL_ZK_PREFIX_DEFAULT);
    }


    /**
     * Get lock timeout
     *
     * @return lock timeout
     */
    public long getLockTimeoutMilliSeconds() {
        return this.getLong(BKDL_LOCK_TIMEOUT, BKDL_LOCK_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set lock timeout
     *
     * @param lockTimeout lock timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setLockTimeout(long lockTimeout) {
        setProperty(BKDL_LOCK_TIMEOUT, lockTimeout);
        return this;
    }

    /**
     * Get BK client read timeout
     *
     * @return read timeout
     */
    public int getBKClientReadTimeout() {
        return this.getInt(BKDL_BKCLIENT_READ_TIMEOUT, BKDL_BKCLIENT_READ_TIMEOUT_DEFAULT);
    }

    /**
     * Set BK client read timeout
     *
     * @param readTimeout read timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setBKClientReadTimeout(int readTimeout) {
        setProperty(BKDL_BKCLIENT_READ_TIMEOUT, readTimeout);
        return this;
    }

    /**
     * Get BK client number of worker threads.
     *
     * @return number of bookkeeper client worker threads.
     */
    public int getBKClientNumberWorkerThreads() {
        return this.getInt(BKDL_BKCLIENT_NUM_WORKER_THREADS, BKDL_BKCLEINT_NUM_WORKER_THREADS_DEFAULT);
    }

    /**
     * Set BK client number of worker threads.
     *
     * @param numThreads
     *          number worker threads.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setBKClientNumberWorkerThreads(int numThreads) {
        setProperty(BKDL_BKCLIENT_NUM_WORKER_THREADS, numThreads);
        return this;
    }

    /**
     * Get ZK client number of retry executor threads.
     *
     * @return number of bookkeeper client worker threads.
     */
    public int getZKClientNumberRetryThreads() {
        return this.getInt(BKDL_ZKCLIENT_NUM_RETRY_THREADS, BKDL_ZKCLIENT_NUM_RETRY_THREADS_DEFAULT);
    }

    /**
     * Set ZK client number of retry executor threads.
     *
     * @param numThreads
     *          number of retry executor threads.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setZKClientNumberRetryThreads(int numThreads) {
        setProperty(BKDL_ZKCLIENT_NUM_RETRY_THREADS, numThreads);
        return this;
    }


    /**
     * Get BK client read timeout
     *
     * @return session timeout in milliseconds
     */
    public int getBKClientZKSessionTimeoutMilliSeconds() {
        return this.getInt(BKDL_BKCLIENT_ZK_SESSION_TIMEOUT, BKDL_BKCLIENT_ZK_SESSION_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set the ZK Session Timeout used by the BK Client
     *
     * @param sessionTimeout session timeout for the ZK Client used by BK Client
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setBKClientZKSessionTimeout(int sessionTimeout) {
        setProperty(BKDL_BKCLIENT_ZK_SESSION_TIMEOUT, sessionTimeout);
        return this;
    }

    /**
     * Get BK client read timeout
     *
     * @return read timeout
     */
    public int getBKClientWriteTimeout() {
        return this.getInt(BKDL_BKCLIENT_WRITE_TIMEOUT, BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);
    }

    /**
     * Set BK client read timeout
     *
     * @param writeTimeout write timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setBKClientWriteTimeout(int writeTimeout) {
        setProperty(BKDL_BKCLIENT_WRITE_TIMEOUT, writeTimeout);
        return this;
    }

    /**
     * Set if we should sanity check before deleting
     *
     * @param sanityCheckDeletes check before deleting
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setSanityCheckDeletes(boolean sanityCheckDeletes) {
        setProperty(BKDL_SANITYCHECK_BEFORE_DELETE, sanityCheckDeletes);
        return this;
    }

    /**
     * Whether sanity check is enabled before deletes
     *
     * @return if we should check before deleting
     */
    public boolean getSanityCheckDeletes() {
        return getBoolean(BKDL_SANITYCHECK_BEFORE_DELETE, BKDL_SANITYCHECK_BEFORE_DELETE_DEFAULT);
    }

    /**
     * Whether we should publish record counts in the log records and metadata
     *
     * @return if record counts should be persisted
     */
    public boolean getEnableRecordCounts() {
        return getBoolean(BKDL_ENABLE_RECORD_COUNTS, BKDL_ENABLE_RECORD_COUNTS_DEFAULT);
    }

    /**
     * Set if we should publish record counts in the log records and metadata
     *
     * @param enableRecordCounts enable record counts
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setEnableRecordCounts(boolean enableRecordCounts) {
        setProperty(BKDL_ENABLE_RECORD_COUNTS, enableRecordCounts);
        return this;
    }

    /**
     * Get the time in milliseconds as the threshold for when an idle reader should dump warnings
     *
     * @return if record counts should be persisted
     */
    public int getReaderIdleWarnThresholdMillis() {
        return getInt(BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS, BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS_DEFAULT);
    }

    /**
     * Set the time in milliseconds as the threshold for when an idle reader should dump warnings
     *
     * @param warnThreshold time after which we should dump the read ahead state
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setReaderIdleWarnThresholdMillis(int warnThreshold) {
        setProperty(BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS, warnThreshold);
        return this;
    }

    /**
     * Get the tick duration in milliseconds that used for timeout timer.
     *
     * @return tick duration in milliseconds
     */
    public long getTimeoutTimerTickDurationMs() {
        return getLong(BKDL_TIMEOUT_TIMER_TICK_DURATION_MS, BKDL_TIMEOUT_TIMER_TICK_DURATION_MS_DEFAULT);
    }

    /**
     * Set the tick duration in milliseconds that used for timeout timer.
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return distributed log configuration.
     */
    public DistributedLogConfiguration setTimeoutTimerTickDurationMs(long tickDuration) {
        setProperty(BKDL_TIMEOUT_TIMER_TICK_DURATION_MS, tickDuration);
        return this;
    }

    /**
     * Get number of ticks that used for timeout timer.
     *
     * @return number of ticks that used for timeout timer.
     */
    public int getTimeoutTimerNumTicks() {
        return getInt(BKDL_TIMEOUT_TIMER_NUM_TICKS, BKDL_TIMEOUT_TIMER_NUM_TICKS_DEFAULT);
    }

    /**
     * Set number of ticks that used for timeout timer.
     *
     * @param numTicks
     *          number of ticks that used for timeout timer.
     * @return distributed log configuration.
     */
    public DistributedLogConfiguration setTimeoutTimerNumTicks(int numTicks) {
        setProperty(BKDL_TIMEOUT_TIMER_NUM_TICKS, numTicks);
        return this;
    }


}
