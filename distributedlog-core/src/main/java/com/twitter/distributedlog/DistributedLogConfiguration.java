package com.twitter.distributedlog;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.twitter.distributedlog.bk.QuorumConfig;
import com.twitter.distributedlog.feature.DefaultFeatureProvider;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.net.DNSResolverForRacks;
import com.twitter.distributedlog.net.DNSResolverForRows;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * DistributedLog Configuration.
 * <p>
 * DistributedLog configuration is basically a properties based configuration, which extends from
 * Apache commons {@link CompositeConfiguration}. All the DL settings are in camel case and prefixed
 * with a meaningful component name. for example, `zkSessionTimeoutSeconds` means <i>SessionTimeoutSeconds</i>
 * for component `zk`.
 *
 * <h3>BookKeeper Configuration</h3>
 *
 * BookKeeper client configuration settings could be loaded via DistributedLog configuration. All those
 * settings are prefixed with <i>`bkc.`</i>. For example, <i>bkc.zkTimeout</i> in distributedlog configuration
 * will be applied as <i>`zkTimeout`</i> in bookkeeper client configuration.
 *
 * <h3>How to load configuration</h3>
 *
 * The default distributedlog configuration is constructed by instantiated a new instance. This
 * distributedlog configuration will automatically load the settings that specified via
 * {@link SystemConfiguration}.
 *
 * <pre>
 *      DistributedLogConfiguration conf = new DistributedLogConfiguration();
 * </pre>
 *
 * The recommended way is to load configuration from URL that points to a configuration file
 * ({@link #loadConf(URL)}).
 *
 * <pre>
 *      String configFile = "/path/to/distributedlog/conf/file";
 *      DistributedLogConfiguration conf = new DistributedLogConfiguration();
 *      conf.loadConf(new File(configFile).toURI().toURL());
 * </pre>
 *
 * @see org.apache.bookkeeper.conf.ClientConfiguration
 */
public class DistributedLogConfiguration extends CompositeConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogConfiguration.class);

    private static ClassLoader defaultLoader;

    static {
        defaultLoader = Thread.currentThread().getContextClassLoader();
        if (null == defaultLoader) {
            defaultLoader = DistributedLogConfiguration.class.getClassLoader();
        }
    }

    //
    // ZooKeeper Related Settings
    //

    public static final String BKDL_ZK_ACL_ID = "zkAclId";
    public static final String BKDL_ZK_ACL_ID_DEFAULT = null;
    public static final String BKDL_ZK_SESSION_TIMEOUT_SECONDS = "zkSessionTimeoutSeconds";
    public static final int BKDL_ZK_SESSION_TIMEOUT_SECONDS_DEFAULT = 30;
    public static final String BKDL_ZK_REQUEST_RATE_LIMIT = "zkRequestRateLimit";
    public static final double BKDL_ZK_REQUEST_RATE_LIMIT_DEFAULT = 0;
    public static final String BKDL_ZK_NUM_RETRIES = "zkNumRetries";
    public static final int BKDL_ZK_NUM_RETRIES_DEFAULT = 3;
    public static final String BKDL_ZK_RETRY_BACKOFF_START_MILLIS = "zkRetryStartBackoffMillis";
    public static final int BKDL_ZK_RETRY_BACKOFF_START_MILLIS_DEFAULT = 5000;
    public static final String BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS = "zkRetryMaxBackoffMillis";
    public static final int BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS_DEFAULT = 30000;
    public static final String BKDL_ZKCLIENT_NUM_RETRY_THREADS = "zkcNumRetryThreads";
    public static final int BKDL_ZKCLIENT_NUM_RETRY_THREADS_DEFAULT = 1;

    //
    // BookKeeper Related Settings
    //

    // BookKeeper zookeeper settings
    public static final String BKDL_BKCLIENT_ZK_SESSION_TIMEOUT = "bkcZKSessionTimeoutSeconds";
    public static final int BKDL_BKCLIENT_ZK_SESSION_TIMEOUT_DEFAULT = 30;
    public static final String BKDL_BKCLIENT_ZK_REQUEST_RATE_LIMIT = "bkcZKRequestRateLimit";
    public static final double BKDL_BKCLIENT_ZK_REQUEST_RATE_LIMIT_DEFAULT = 0;
    public static final String BKDL_BKCLIENT_ZK_NUM_RETRIES = "bkcZKNumRetries";
    public static final int BKDL_BKCLIENT_ZK_NUM_RETRIES_DEFAULT = 3;
    public static final String BKDL_BKCLIENT_ZK_RETRY_BACKOFF_START_MILLIS = "bkcZKRetryStartBackoffMillis";
    public static final int BKDL_BKCLIENT_ZK_RETRY_BACKOFF_START_MILLIS_DEFAULT = 5000;
    public static final String BKDL_BKCLIENT_ZK_RETRY_BACKOFF_MAX_MILLIS = "bkcZKRetryMaxBackoffMillis";
    public static final int BKDL_BKCLIENT_ZK_RETRY_BACKOFF_MAX_MILLIS_DEFAULT = 30000;

    // Bookkeeper ensemble placement settings
    // Bookkeeper ensemble size
    public static final String BKDL_BOOKKEEPER_ENSEMBLE_SIZE = "bkcEnsembleSize";
    // @Deprecated
    public static final String BKDL_BOOKKEEPER_ENSEMBLE_SIZE_OLD = "ensemble-size";
    public static final int BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT = 3;
    // Bookkeeper write quorum size
    public static final String BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE = "bkcWriteQuorumSize";
    // @Deprecated
    public static final String BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_OLD = "write-quorum-size";
    public static final int BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT = 3;
    // Bookkeeper ack quorum size
    public static final String BKDL_BOOKKEEPER_ACK_QUORUM_SIZE = "bkcAckQuorumSize";
    // @Deprecated
    public static final String BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_OLD = "ack-quorum-size";
    public static final int BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT = 2;
    public static final String BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT = "bkRowAwareEnsemblePlacement";
    public static final String BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT_OLD = "row-aware-ensemble-placement";
    public static final boolean BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT_DEFAULT = false;
    public static final String BKDL_ENSEMBLE_PLACEMENT_DNS_RESOLVER_CLASS = "bkEnsemblePlacementDnsResolverClass";
    public static final String BKDL_ENSEMBLE_PLACEMENT_DNS_RESOLVER_CLASS_DEFAULT =
            DNSResolverForRacks.class.getName();
    public static final String BKDL_BK_DNS_RESOLVER_OVERRIDES = "dnsResolverOverrides";
    public static final String BKDL_BK_DNS_RESOLVER_OVERRIDES_DEFAULT = "";

    // General Settings
    // @Deprecated
    public static final String BKDL_BOOKKEEPER_DIGEST_PW = "digestPw";
    public static final String BKDL_BOOKKEEPER_DIGEST_PW_DEFAULT = "";
    public static final String BKDL_BKCLIENT_NUM_IO_THREADS = "bkcNumIOThreads";
    public static final String BKDL_TIMEOUT_TIMER_TICK_DURATION_MS = "timerTickDuration";
    public static final long BKDL_TIMEOUT_TIMER_TICK_DURATION_MS_DEFAULT = 100;
    public static final String BKDL_TIMEOUT_TIMER_NUM_TICKS = "timerNumTicks";
    public static final int BKDL_TIMEOUT_TIMER_NUM_TICKS_DEFAULT = 1024;

    //
    // Deprecated BookKeeper Settings (in favor of "bkc." style bookkeeper settings)
    //

    public static final String BKDL_BKCLIENT_READ_TIMEOUT = "bkcReadTimeoutSeconds";
    public static final int BKDL_BKCLIENT_READ_TIMEOUT_DEFAULT = 10;
    public static final String BKDL_BKCLIENT_WRITE_TIMEOUT = "bkcWriteTimeoutSeconds";
    public static final int BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT = 10;
    public static final String BKDL_BKCLIENT_NUM_WORKER_THREADS = "bkcNumWorkerThreads";
    public static final int BKDL_BKCLEINT_NUM_WORKER_THREADS_DEFAULT = 1;

    //
    // DL General Settings
    //

    // Executor Parameters
    public static final String BKDL_NUM_WORKER_THREADS = "numWorkerThreads";
    public static final String BKDL_NUM_READAHEAD_WORKER_THREADS = "numReadAheadWorkerThreads";
    public static final String BKDL_NUM_LOCKSTATE_THREADS = "numLockStateThreads";
    public static final String BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS = "schedulerShutdownTimeoutMs";
    public static final int BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS_DEFAULT = 5000;
    public static final String BKDL_USE_DAEMON_THREAD = "useDaemonThread";
    public static final boolean BKDL_USE_DAEMON_THREAD_DEFAULT = false;

    // Metadata Parameters
    public static final String BKDL_LEDGER_METADATA_LAYOUT_VERSION = "ledgerMetadataLayoutVersion";
    public static final String BKDL_LEDGER_METADATA_LAYOUT_VERSION_OLD = "ledger-metadata-layout";
    public static final int BKDL_LEDGER_METADATA_LAYOUT_VERSION_DEFAULT =
            LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value;
    public static final String BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK = "ledgerMetadataSkipMinVersionCheck";
    public static final boolean BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK_DEFAULT = false;
    public static final String BKDL_FIRST_LOGSEGMENT_SEQUENCE_NUMBER = "firstLogsegmentSequenceNumber";
    public static final String BKDL_FIRST_LOGSEGMENT_SEQUENCE_NUMBER_OLD = "first-logsegment-sequence-number";
    public static final long BKDL_FIRST_LOGSEGMENT_SEQUENCE_NUMBER_DEFAULT =
            DistributedLogConstants.FIRST_LOGSEGMENT_SEQNO;
    public static final String BKDL_ENABLE_RECORD_COUNTS = "enableRecordCounts";
    public static final boolean BKDL_ENABLE_RECORD_COUNTS_DEFAULT = true;
    public static final String BKDL_MAXID_SANITYCHECK = "maxIdSanityCheck";
    public static final boolean BKDL_MAXID_SANITYCHECK_DEFAULT = true;
    public static final String BKDL_ENCODE_REGION_ID_IN_VERSION = "encodeRegionIDInVersion";
    public static final boolean BKDL_ENCODE_REGION_ID_IN_VERSION_DEFAULT = false;
    // (@Deprecated)
    public static final String BKDL_LOGSEGMENT_NAME_VERSION = "logSegmentNameVersion";
    public static final int BKDL_LOGSEGMENT_NAME_VERSION_DEFAULT = DistributedLogConstants.LOGSEGMENT_NAME_VERSION;
    // (@Derepcated) Name for the default (non-partitioned) stream
    public static final String BKDL_UNPARTITIONED_STREAM_NAME = "unpartitionedStreamName";
    public static final String BKDL_UNPARTITIONED_STREAM_NAME_DEFAULT = "<default>";

    //
    // DL Writer Settings
    //

    // General Settings
    public static final String BKDL_CREATE_STREAM_IF_NOT_EXISTS = "createStreamIfNotExists";
    public static final boolean BKDL_CREATE_STREAM_IF_NOT_EXISTS_DEFAULT = true;
    public static final String BKDL_LOG_FLUSH_TIMEOUT = "logFlushTimeoutSeconds";
    public static final int BKDL_LOG_FLUSH_TIMEOUT_DEFAULT = 30;
    /**
     *  CompressionCodec.Type     String to use (See CompressionUtils)
     *  ---------------------     ------------------------------------
     *          NONE               none
     *          LZ4                lz4
     *          UNKNOWN            any other instance of String.class
     */
    public static final String BKDL_COMPRESSION_TYPE = "compressionType";
    public static final String BKDL_COMPRESSION_TYPE_DEFAULT = "none";
    public static final String BKDL_FAILFAST_ON_STREAM_NOT_READY = "failFastOnStreamNotReady";
    public static final boolean BKDL_FAILFAST_ON_STREAM_NOT_READY_DEFAULT = false;
    public static final String BKDL_DISABLE_ROLLING_ON_LOG_SEGMENT_ERROR = "disableRollingOnLogSegmentError";
    public static final boolean BKDL_DISABLE_ROLLING_ON_LOG_SEGMENT_ERROR_DEFAULT = false;

    // Durability Settings
    public static final String BKDL_IS_DURABLE_WRITE_ENABLED = "isDurableWriteEnabled";
    public static final boolean BKDL_IS_DURABLE_WRITE_ENABLED_DEFAULT = true;

    // Transmit Settings
    public static final String BKDL_OUTPUT_BUFFER_SIZE = "writerOutputBufferSize";
    public static final String BKDL_OUTPUT_BUFFER_SIZE_OLD = "output-buffer-size";
    public static final int BKDL_OUTPUT_BUFFER_SIZE_DEFAULT = 1024;
    public static final String BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS = "periodicFlushFrequencyMilliSeconds";
    public static final int BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT = 0;
    public static final String BKDL_ENABLE_IMMEDIATE_FLUSH = "enableImmediateFlush";
    public static final boolean BKDL_ENABLE_IMMEDIATE_FLUSH_DEFAULT = false;
    public static final String BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS = "minimumDelayBetweenImmediateFlushMilliSeconds";
    public static final int BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS_DEFAULT = 0;

    // Retention/Truncation Settings
    public static final String BKDL_RETENTION_PERIOD_IN_HOURS = "logSegmentRetentionHours";
    public static final String BKDL_RETENTION_PERIOD_IN_HOURS_OLD = "retention-size";
    public static final int BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT = 72;
    public static final String BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION = "explicitTruncationByApp";
    public static final boolean BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION_DEFAULT = false;

    // Log Segment Rolling Settings
    public static final String BKDL_ROLLING_INTERVAL_IN_MINUTES = "logSegmentRollingMinutes";
    public static final String BKDL_ROLLING_INTERVAL_IN_MINUTES_OLD = "rolling-interval";
    public static final int BKDL_ROLLING_INTERVAL_IN_MINUTES_DEFAULT = 120;
    public static final String BKDL_MAX_LOGSEGMENT_BYTES = "maxLogSegmentBytes";
    public static final int BKDL_MAX_LOGSEGMENT_BYTES_DEFAULT = 256 * 1024 * 1024; // default 256MB
    public static final String BKDL_LOGSEGMENT_ROLLING_CONCURRENCY = "logSegmentRollingConcurrency";
    public static final int BKDL_LOGSEGMENT_ROLLING_CONCURRENCY_DEFAULT = 1;

    // Lock Settings
    public static final String BKDL_LOCK_TIMEOUT = "lockTimeoutSeconds";
    public static final long BKDL_LOCK_TIMEOUT_DEFAULT = 30;
    public static final String BKDL_LOCK_REACQUIRE_TIMEOUT = "lockReacquireTimeoutSeconds";
    public static final long BKDL_LOCK_REACQUIRE_TIMEOUT_DEFAULT = DistributedLogConstants.LOCK_REACQUIRE_TIMEOUT_DEFAULT;
    public static final String BKDL_LOCK_OP_TIMEOUT = "lockOpTimeoutSeconds";
    public static final long BKDL_LOCK_OP_TIMEOUT_DEFAULT = DistributedLogConstants.LOCK_OP_TIMEOUT_DEFAULT;

    // Ledger Allocator Settings
    public static final String BKDL_ENABLE_LEDGER_ALLOCATOR_POOL = "enableLedgerAllocatorPool";
    public static final boolean BKDL_ENABLE_LEDGER_ALLOCATOR_POOL_DEFAULT = false;
    public static final String BKDL_LEDGER_ALLOCATOR_POOL_PATH = "ledgerAllocatorPoolPath";
    public static final String BKDL_LEDGER_ALLOCATOR_POOL_PATH_DEFAULT = DistributedLogConstants.ALLOCATION_POOL_NODE;
    public static final String BKDL_LEDGER_ALLOCATOR_POOL_NAME = "ledgerAllocatorPoolName";
    public static final String BKDL_LEDGER_ALLOCATOR_POOL_NAME_DEFAULT = null;
    public static final String BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE = "ledgerAllocatorPoolCoreSize";
    public static final int BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE_DEFAULT = 20;

    // Write Limit Settings
    public static final String BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT = "perWriterOutstandingWriteLimit";
    public static final int BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT = "globalOutstandingWriteLimit";
    public static final int BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE = "outstandingWriteLimitDarkmode";
    public static final boolean BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE_DEFAULT = true;

    //
    // DL Reader Settings
    //

    // General Settings
    public static final String BKDL_READLAC_OPTION = "readLACLongPoll";
    public static final int BKDL_READLAC_OPTION_DEFAULT = 3; //BKLogPartitionReadHandler.ReadLACOption.READENTRYPIGGYBACK_SEQUENTIAL.value
    public static final String BKDL_READLACLONGPOLL_TIMEOUT = "readLACLongPollTimeout";
    public static final int BKDL_READLACLONGPOLL_TIMEOUT_DEFAULT = 1000;

    // Idle reader settings
    public static final String BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS = "readerIdleWarnThresholdMillis";
    public static final int BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS_DEFAULT = 120000;
    public static final String BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS = "readerIdleErrorThresholdMillis";
    public static final int BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT = Integer.MAX_VALUE;

    // Reader constraint settings
    public static final String BKDL_READER_IGNORE_TRUNCATION_STATUS = "ignoreTruncationStatus";
    public static final boolean BKDL_READER_IGNORE_TRUNCATION_STATUS_DEFAULT = false;
    public static final String BKDL_READER_ALERT_POSITION_ON_TRUNCATED = "alertPositionOnTruncated";
    public static final boolean BKDL_READER_ALERT_POSITION_ON_TRUNCATED_DEFAULT = true;
    public static final String BKDL_READER_POSITION_GAP_DETECTION_ENABLED = "positionGapDetectionEnabled";
    public static final boolean BKDL_READER_POSITION_GAP_DETECTION_ENABLED_DEFAULT = false;

    // Read ahead related parameters
    public static final String BKDL_ENABLE_READAHEAD = "enableReadAhead";
    public static final boolean BKDL_ENABLE_READAHEAD_DEFAULT = true;
    public static final String BKDL_ENABLE_FORCEREAD = "enableForceRead";
    public static final boolean BKDL_ENABLE_FORCEREAD_DEFAULT = true;
    public static final String BKDL_READAHEAD_MAX_RECORDS = "readAheadMaxRecords";
    public static final String BKDL_READAHEAD_MAX_RECORDS_OLD = "ReadAheadMaxEntries";
    public static final int BKDL_READAHEAD_MAX_RECORDS_DEFAULT = 10;
    public static final String BKDL_READAHEAD_BATCHSIZE = "readAheadBatchSize";
    public static final String BKDL_READAHEAD_BATCHSIZE_OLD = "ReadAheadBatchSize";
    public static final int BKDL_READAHEAD_BATCHSIZE_DEFAULT = 2;
    public static final String BKDL_READAHEAD_WAITTIME = "readAheadWaitTime";
    public static final String BKDL_READAHEAD_WAITTIME_OLD = "ReadAheadWaitTime";
    public static final int BKDL_READAHEAD_WAITTIME_DEFAULT = 200;
    public static final String BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM = "readAheadWaitTimeOnEndOfStream";
    public static final String BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM_OLD = "ReadAheadWaitTimeOnEndOfStream";
    public static final int BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM_DEFAULT = 10000;
    public static final String BKDL_READAHEAD_NOSUCHLEDGER_EXCEPTION_ON_READLAC_ERROR_THRESHOLD_MILLIS =
            "readAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis";
    public static final int BKDL_READAHEAD_NOSUCHLEDGER_EXCEPTION_ON_READLAC_ERROR_THRESHOLD_MILLIS_DEFAULT = 10000;
    public static final String BKDL_READAHEAD_SKIP_BROKEN_ENTRIES = "readAheadSkipBrokenEntries";
    public static final boolean BKDL_READAHEAD_SKIP_BROKEN_ENTRIES_DEFAULT = false;

    // Scan Settings
    public static final String BKDL_FIRST_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN = "firstNumEntriesEachPerLastRecordScan";
    public static final int BKDL_FIRST_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN_DEFAULT = 2;
    public static final String BKDL_MAX_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN = "maxNumEntriesPerReadLastRecordScan";
    public static final int BKDL_MAX_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN_DEFAULT = 16;

    // Log Existence Settings
    public static final String BKDL_CHECK_LOG_EXISTENCE_BACKOFF_START_MS = "checkLogExistenceBackoffStartMillis";
    public static final int BKDL_CHECK_LOG_EXISTENCE_BACKOFF_START_MS_DEFAULT = 200;
    public static final String BKDL_CHECK_LOG_EXISTENCE_BACKOFF_MAX_MS = "checkLogExistenceBackoffMaxMillis";
    public static final int BKDL_CHECK_LOG_EXISTENCE_BACKOFF_MAX_MS_DEFAULT = 1000;

    //
    // Tracing/Stats Settings
    //

    public static final String BKDL_TRACE_READAHEAD_DELIVERY_LATENCY = "traceReadAheadDeliveryLatency";
    public static final boolean BKDL_TRACE_READAHEAD_DELIVERY_LATENCY_DEFAULT = false;
    public static final String BKDL_METADATA_LATENCY_WARN_THRESHOLD_MS = "metadataLatencyWarnThresholdMs";
    public static final long BKDL_METADATA_LATENCY_WARN_THRESHOLD_MS_DEFAULT = DistributedLogConstants.LATENCY_WARN_THRESHOLD_IN_MILLIS;
    public static final String BKDL_DATA_LATENCY_WARN_THRESHOLD_MS = "dataLatencyWarnThresholdMs";
    public static final long BKDL_DATA_LATENCY_WARN_THRESHOLD_MS_DEFAULT = 2 * DistributedLogConstants.LATENCY_WARN_THRESHOLD_IN_MILLIS;
    public static final String BKDL_TRACE_READAHEAD_METADATA_CHANGES = "traceReadAheadMetadataChanges";
    public static final boolean BKDL_TRACE_READAHEAD_MEATDATA_CHANGES_DEFAULT = false;
    public final static String BKDL_ENABLE_TASK_EXECUTION_STATS = "enableTaskExecutionStats";
    public final static boolean BKDL_ENABLE_TASK_EXECUTION_STATS_DEFAULT = false;
    public final static String BKDL_TASK_EXECUTION_WARN_TIME_MICROS = "taskExecutionWarnTimeMicros";
    public final static long BKDL_TASK_EXECUTION_WARN_TIME_MICROS_DEFAULT = 100000;
    public static final String BKDL_ENABLE_PERSTREAM_STAT = "enablePerStreamStat";
    public static final boolean BKDL_ENABLE_PERSTREAM_STAT_DEFAULT = false;

    //
    // Settings for Feature Providers
    //

    public static final String BKDL_FEATURE_PROVIDER_CLASS = "featureProviderClass";

    //
    // Settings for Configuration Based Feature Provider
    //

    public static final String BKDL_FILE_FEATURE_PROVIDER_BASE_CONFIG_PATH = "fileFeatureProviderBaseConfigPath";
    public static final String BKDL_FILE_FEATURE_PROVIDER_BASE_CONFIG_PATH_DEFAULT = "decider.yml";
    public static final String BKDL_FILE_FEATURE_PROVIDER_OVERLAY_CONFIG_PATH = "fileFeatureProviderOverlayConfigPath";
    public static final String BKDL_FILE_FEATURE_PROVIDER_OVERLAY_CONFIG_PATH_DEFAULT = null;

    //
    // Settings for Namespaces
    //

    public static final String BKDL_FEDERATED_NAMESPACE_ENABLED = "federatedNamespaceEnabled";
    public static final boolean BKDL_FEDERATED_NAMESPACE_ENABLED_DEFAULT = false;
    public static final String BKDL_FEDERATED_MAX_LOGS_PER_SUBNAMESPACE = "federatedMaxLogsPerSubnamespace";
    public static final int BKDL_FEDERATED_MAX_LOGS_PER_SUBNAMESPACE_DEFAULT = 15000;
    public static final String BKDL_FEDERATED_CHECK_EXISTENCE_WHEN_CACHE_MISS = "federatedCheckExistenceWhenCacheMiss";
    public static final boolean BKDL_FEDERATED_CHECK_EXISTENCE_WHEN_CACHE_MISS_DEFAULT = true;

    // Settings for Configurations

    public static final String BKDL_DYNAMIC_CONFIG_RELOAD_INTERVAL_SEC = "dynamicConfigReloadIntervalSec";
    public static final int BKDL_DYNAMIC_CONFIG_RELOAD_INTERVAL_SEC_DEFAULT = 60;
    public static final String BKDL_STREAM_CONFIG_ROUTER_CLASS = "streamConfigRouterClass";
    public static final String BKDL_STREAM_CONFIG_ROUTER_CLASS_DEFAULT = "com.twitter.distributedlog.service.config.IdentityConfigRouter";

    // Settings for RateLimit (used by distributedlog-service)

    public static final String BKDL_BPS_SOFT_WRITE_LIMIT = "bpsSoftWriteLimit";
    public static final int BKDL_BPS_SOFT_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_BPS_HARD_WRITE_LIMIT = "bpsHardWriteLimit";
    public static final int BKDL_BPS_HARD_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_RPS_SOFT_WRITE_LIMIT = "rpsSoftWriteLimit";
    public static final int BKDL_RPS_SOFT_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_RPS_HARD_WRITE_LIMIT = "rpsHardWriteLimit";
    public static final int BKDL_RPS_HARD_WRITE_LIMIT_DEFAULT = -1;

    // Rate and resource limits: per shard

    public static final String BKDL_RPS_SOFT_SERVICE_LIMIT = "rpsSoftServiceLimit";
    public static final int BKDL_RPS_SOFT_SERVICE_LIMIT_DEFAULT = -1;
    public static final String BKDL_RPS_HARD_SERVICE_LIMIT = "rpsHardServiceLimit";
    public static final int BKDL_RPS_HARD_SERVICE_LIMIT_DEFAULT = -1;
    public static final String BKDL_RPS_STREAM_ACQUIRE_SERVICE_LIMIT = "rpsStreamAcquireServiceLimit";
    public static final int BKDL_RPS_STREAM_ACQUIRE_SERVICE_LIMIT_DEFAULT = -1;
    public static final String BKDL_BPS_SOFT_SERVICE_LIMIT = "bpsSoftServiceLimit";
    public static final int BKDL_BPS_SOFT_SERVICE_LIMIT_DEFAULT = -1;
    public static final String BKDL_BPS_HARD_SERVICE_LIMIT = "bpsHardServiceLimit";
    public static final int BKDL_BPS_HARD_SERVICE_LIMIT_DEFAULT = -1;
    public static final String BKDL_BPS_STREAM_ACQUIRE_SERVICE_LIMIT = "bpsStreamAcquireServiceLimit";
    public static final int BKDL_BPS_STREAM_ACQUIRE_SERVICE_LIMIT_DEFAULT = -1;

    // Settings for Partitioning

    public static final String BKDL_MAX_ACQUIRED_PARTITIONS_PER_PROXY = "maxAcquiredPartitionsPerProxy";
    public static final int BKDL_MAX_ACQUIRED_PARTITIONS_PER_PROXY_DEFAULT = -1;

    public static final String BKDL_MAX_CACHED_PARTITIONS_PER_PROXY = "maxCachedPartitionsPerProxy";
    public static final int BKDL_MAX_CACHED_PARTITIONS_PER_PROXY_DEFAULT = -1;

    //
    // Settings for Error Injection
    //
    public static final String BKDL_EI_INJECT_WRITE_DELAY = "eiInjectWriteDelay";
    public static final boolean BKDL_EI_INJECT_WRITE_DELAY_DEFAULT = false;
    public static final String BKDL_EI_INJECTED_WRITE_DELAY_PERCENT = "eiInjectedWriteDelayPercent";
    public static final double BKDL_EI_INJECTED_WRITE_DELAY_PERCENT_DEFAULT = 0.0;
    public static final String BKDL_EI_INJECTED_WRITE_DELAY_MS = "eiInjectedWriteDelayMs";
    public static final int BKDL_EI_INJECTED_WRITE_DELAY_MS_DEFAULT = 0;
    public static final String BKDL_EI_INJECT_READAHEAD_STALL = "eiInjectReadAheadStall";
    public static final boolean BKDL_EI_INJECT_READAHEAD_STALL_DEFAULT = false;
    public static final String BKDL_EI_INJECT_READAHEAD_DELAY = "eiInjectReadAheadDelay";
    public static final boolean BKDL_EI_INJECT_READAHEAD_DELAY_DEFAULT = false;
    public static final String BKDL_EI_INJECT_MAX_READAHEAD_DELAY_MS = "eiInjectMaxReadAheadDelayMs";
    public static final int BKDL_EI_INJECT_MAX_READAHEAD_DELAY_MS_DEFAULT = 0;
    public static final String BKDL_EI_INJECT_READAHEAD_DELAY_PERCENT = "eiInjectReadAheadDelayPercent";
    public static final int BKDL_EI_INJECT_READAHEAD_DELAY_PERCENT_DEFAULT = 10;
    public static final String BKDL_EI_INJECT_READAHEAD_BROKEN_ENTRIES = "eiInjectReadAheadBrokenEntries";
    public static final boolean BKDL_EI_INJECT_READAHEAD_BROKEN_ENTRIES_DEFAULT = false;

    // Whitelisted stream-level configuration settings.
    private static final Set<String> streamSettings = Sets.newHashSet(
        BKDL_READER_POSITION_GAP_DETECTION_ENABLED,
        BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS,
        BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS,
        BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS,
        BKDL_ENABLE_IMMEDIATE_FLUSH
    );

    /**
     * Construct distributedlog configuration with default settings.
     * It also loads the settings from system properties.
     */
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
     * Load whitelisted stream configuration from another configuration object
     *
     * @param streamConfiguration stream configuration overrides
     */
    public void loadStreamConf(Optional<DistributedLogConfiguration> streamConfiguration) {
        if (!streamConfiguration.isPresent()) {
            return;
        }
        ArrayList<Object> ignoredSettings = new ArrayList<Object>();
        Iterator iterator = streamConfiguration.get().getKeys();
        while (iterator.hasNext()) {
            Object setting = iterator.next();
            if (setting instanceof String && streamSettings.contains(setting)) {
                String settingStr = (String) setting;
                setProperty(settingStr, streamConfiguration.get().getProperty(settingStr));
            } else {
                ignoredSettings.add(setting);
            }
        }
        if (LOG.isWarnEnabled() && !ignoredSettings.isEmpty()) {
            LOG.warn("invalid stream configuration override(s): {}",
                StringUtils.join(ignoredSettings, ";"));
        }
    }

    //
    // ZooKeeper Related Settings
    //

    /**
     * Get all properties as a string.
     */
    public String getPropsAsString() {
        Iterator iterator = getKeys();
        StringBuilder builder = new StringBuilder();
        boolean appendNewline = false;
        while (iterator.hasNext()) {
            Object key = iterator.next();
            if (key instanceof String) {
                if (appendNewline) {
                    builder.append("\n");
                }
                Object value = getProperty((String)key);
                builder.append(key).append("=").append(value);
                appendNewline = true;
            }
        }
        return builder.toString();
    }

    /**
     * Get digest id used for ZK acl.
     *
     * @return zk acl id.
     */
    public String getZkAclId() {
        return getString(BKDL_ZK_ACL_ID, BKDL_ZK_ACL_ID_DEFAULT);
    }

    /**
     * Set digest id to use for ZK acl.
     *
     * @param zkAclId acl id.
     * @return distributedlog configuration
     * @see #getZkAclId()
     */
    public DistributedLogConfiguration setZkAclId(String zkAclId) {
        setProperty(BKDL_ZK_ACL_ID, zkAclId);
        return this;
    }

    /**
     * Get ZK Session timeout in seconds.
     * <p>
     * This is the session timeout applied for zookeeper client used by distributedlog.
     * Use {@link #getBKClientZKSessionTimeoutMilliSeconds()} for zookeeper client used
     * by bookkeeper client.
     *
     * @return zookeeeper session timeout in seconds.
     * @deprecated use {@link #getZKSessionTimeoutMilliseconds()}
     */
    public int getZKSessionTimeoutSeconds() {
        return this.getInt(BKDL_ZK_SESSION_TIMEOUT_SECONDS, BKDL_ZK_SESSION_TIMEOUT_SECONDS_DEFAULT);
    }

    /**
     * Get ZK Session timeout in milliseconds.
     * <p>
     * This is the session timeout applied for zookeeper client used by distributedlog.
     * Use {@link #getBKClientZKSessionTimeoutMilliSeconds()} for zookeeper client used
     * by bookkeeper client.
     *
     * @return zk session timeout in milliseconds.
     */
    public int getZKSessionTimeoutMilliseconds() {
        return this.getInt(BKDL_ZK_SESSION_TIMEOUT_SECONDS, BKDL_ZK_SESSION_TIMEOUT_SECONDS_DEFAULT) * 1000;
    }

    /**
     * Set ZK Session Timeout in seconds.
     *
     * @param zkSessionTimeoutSeconds session timeout in seconds.
     * @return distributed log configuration
     * @see #getZKSessionTimeoutMilliseconds()
     */
    public DistributedLogConfiguration setZKSessionTimeoutSeconds(int zkSessionTimeoutSeconds) {
        setProperty(BKDL_ZK_SESSION_TIMEOUT_SECONDS, zkSessionTimeoutSeconds);
        return this;
    }

    /**
     * Get zookeeper access rate limit.
     * <p>The rate limiter is basically a guava {@link com.google.common.util.concurrent.RateLimiter}.
     * It is rate limiting the requests that sent by zookeeper client. If the value is non-positive,
     * the rate limiting is disable. By default it is disable (value = 0).
     *
     * @return zookeeper access rate, by default it is 0.
     */
    public double getZKRequestRateLimit() {
        return this.getDouble(BKDL_ZK_REQUEST_RATE_LIMIT, BKDL_ZK_REQUEST_RATE_LIMIT_DEFAULT);
    }

    /**
     * Set zookeeper access rate limit (rps).
     *
     * @param requestRateLimit
     *          zookeeper access rate limit
     * @return distributedlog configuration
     * @see #getZKRequestRateLimit()
     */
    public DistributedLogConfiguration setZKRequestRateLimit(double requestRateLimit) {
        setProperty(BKDL_ZK_REQUEST_RATE_LIMIT, requestRateLimit);
        return this;
    }

    /**
     * Get num of retries per request for zookeeper client.
     * <p>Retries only happen on retryable failures like session expired,
     * session moved. for permanent failures, the request will fail immediately.
     * The default value is 3.
     *
     * @return num of retries per request of zookeeper client.
     */
    public int getZKNumRetries() {
        return this.getInt(BKDL_ZK_NUM_RETRIES, BKDL_ZK_NUM_RETRIES_DEFAULT);
    }

    /**
     * Set num of retries per request for zookeeper client.
     *
     * @param zkNumRetries num of retries per request of zookeeper client.
     * @return distributed log configuration
     * @see #getZKNumRetries()
     */
    public DistributedLogConfiguration setZKNumRetries(int zkNumRetries) {
        setProperty(BKDL_ZK_NUM_RETRIES, zkNumRetries);
        return this;
    }

    /**
     * Get the start backoff time of zookeeper operation retries, in milliseconds.
     * <p>The retry time will increase in bound exponential way, and become flat
     * after hit max backoff time ({@link #getZKRetryBackoffMaxMillis()}).
     * The default start backoff time is 5000 milliseconds.
     *
     * @return start backoff time of zookeeper operation retries, in milliseconds.
     * @see #getZKRetryBackoffMaxMillis()
     */
    public int getZKRetryBackoffStartMillis() {
        return this.getInt(BKDL_ZK_RETRY_BACKOFF_START_MILLIS,
                           BKDL_ZK_RETRY_BACKOFF_START_MILLIS_DEFAULT);
    }

    /**
     * Set the start backoff time of zookeeper operation retries, in milliseconds.
     *
     * @param zkRetryBackoffStartMillis start backoff time of zookeeper operation retries,
     *                                  in milliseconds.
     * @return distributed log configuration
     * @see #getZKRetryBackoffStartMillis()
     */
    public DistributedLogConfiguration setZKRetryBackoffStartMillis(int zkRetryBackoffStartMillis) {
        setProperty(BKDL_ZK_RETRY_BACKOFF_START_MILLIS, zkRetryBackoffStartMillis);
        return this;
    }

    /**
     * Get the max backoff time of zookeeper operation retries, in milliseconds.
     * <p>The retry time will increase in bound exponential way starting from
     * {@link #getZKRetryBackoffStartMillis()}, and become flat after hit this max
     * backoff time.
     * The default max backoff time is 30000 milliseconds.
     *
     * @return max backoff time of zookeeper operation retries, in milliseconds.
     * @see #getZKRetryBackoffStartMillis()
     */
    public int getZKRetryBackoffMaxMillis() {
        return this.getInt(BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS,
                           BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS_DEFAULT);
    }

    /**
     * Set the max backoff time of zookeeper operation retries, in milliseconds.
     *
     * @param zkRetryBackoffMaxMillis max backoff time of zookeeper operation retries,
     *                                in milliseconds.
     * @return distributed log configuration
     * @see #getZKRetryBackoffMaxMillis()
     */
    public DistributedLogConfiguration setZKRetryBackoffMaxMillis(int zkRetryBackoffMaxMillis) {
        setProperty(BKDL_ZK_RETRY_BACKOFF_MAX_MILLIS, zkRetryBackoffMaxMillis);
        return this;
    }

    /**
     * Get ZK client number of retry executor threads.
     * By default it is 1.
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
     * @see #getZKClientNumberRetryThreads()
     */
    public DistributedLogConfiguration setZKClientNumberRetryThreads(int numThreads) {
        setProperty(BKDL_ZKCLIENT_NUM_RETRY_THREADS, numThreads);
        return this;
    }

    //
    // BookKeeper ZooKeeper Client Settings
    //

    /**
     * Get BK's zookeeper session timout in milliseconds.
     * <p>
     * This is the session timeout applied for zookeeper client used by bookkeeper client.
     * Use {@link #getZKSessionTimeoutMilliseconds()} for zookeeper client used
     * by distributedlog.
     *
     * @return Bk's zookeeper session timeout in milliseconds
     */
    public int getBKClientZKSessionTimeoutMilliSeconds() {
        return this.getInt(BKDL_BKCLIENT_ZK_SESSION_TIMEOUT, BKDL_BKCLIENT_ZK_SESSION_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set BK's zookeeper session timeout in seconds.
     *
     * @param sessionTimeout session timeout for the ZK Client used by BK Client, in seconds.
     * @return distributed log configuration
     * @see #getBKClientZKSessionTimeoutMilliSeconds()
     */
    public DistributedLogConfiguration setBKClientZKSessionTimeout(int sessionTimeout) {
        setProperty(BKDL_BKCLIENT_ZK_SESSION_TIMEOUT, sessionTimeout);
        return this;
    }

    /**
     * Get zookeeper access rate limit for zookeeper client used in bookkeeper client.
     * <p>The rate limiter is basically a guava {@link com.google.common.util.concurrent.RateLimiter}.
     * It is rate limiting the requests that sent by zookeeper client. If the value is non-positive,
     * the rate limiting is disable. By default it is disable (value = 0).
     *
     * @return zookeeper access rate limit for zookeeper client used in bookkeeper client.
     * By default it is 0.
     */
    public double getBKClientZKRequestRateLimit() {
        return this.getDouble(BKDL_BKCLIENT_ZK_REQUEST_RATE_LIMIT,
                BKDL_BKCLIENT_ZK_REQUEST_RATE_LIMIT_DEFAULT);
    }

    /**
     * Set zookeeper access rate limit for zookeeper client used in bookkeeper client.
     *
     * @param rateLimit
     *          zookeeper access rate limit
     * @return distributedlog configuration.
     * @see #getBKClientZKRequestRateLimit()
     */
    public DistributedLogConfiguration setBKClientZKRequestRateLimit(double rateLimit) {
        setProperty(BKDL_BKCLIENT_ZK_REQUEST_RATE_LIMIT, rateLimit);
        return this;
    }

    /**
     * Get num of retries for zookeeper client that used by bookkeeper client.
     * <p>Retries only happen on retryable failures like session expired,
     * session moved. for permanent failures, the request will fail immediately.
     * The default value is 3.
     *
     * @return num of retries of zookeeper client used by bookkeeper client.
     */
    public int getBKClientZKNumRetries() {
        return this.getInt(BKDL_BKCLIENT_ZK_NUM_RETRIES, BKDL_BKCLIENT_ZK_NUM_RETRIES_DEFAULT);
    }

    /**
     * Get the start backoff time of zookeeper operation retries, in milliseconds.
     * <p>The retry time will increase in bound exponential way, and become flat
     * after hit max backoff time ({@link #getBKClientZKRetryBackoffMaxMillis()}.
     * The default start backoff time is 5000 milliseconds.
     *
     * @return start backoff time of zookeeper operation retries, in milliseconds.
     * @see #getBKClientZKRetryBackoffMaxMillis()
     */
    public int getBKClientZKRetryBackoffStartMillis() {
        return this.getInt(BKDL_BKCLIENT_ZK_RETRY_BACKOFF_START_MILLIS,
                           BKDL_BKCLIENT_ZK_RETRY_BACKOFF_START_MILLIS_DEFAULT);
    }

    /**
     * Get the max backoff time of zookeeper operation retries, in milliseconds.
     * <p>The retry time will increase in bound exponential way starting from
     * {@link #getBKClientZKRetryBackoffStartMillis()}, and become flat after
     * hit this max backoff time.
     * The default max backoff time is 30000 milliseconds.
     *
     * @return max backoff time of zookeeper operation retries, in milliseconds.
     * @see #getBKClientZKRetryBackoffStartMillis()
     */
    public int getBKClientZKRetryBackoffMaxMillis() {
        return this.getInt(BKDL_BKCLIENT_ZK_RETRY_BACKOFF_MAX_MILLIS,
                BKDL_BKCLIENT_ZK_RETRY_BACKOFF_MAX_MILLIS_DEFAULT);
    }

    //
    // BookKeeper Ensemble Placement Settings
    //

    /**
     * Get ensemble size of each log segment (ledger) will use.
     * By default it is 3.
     * <p>
     * A log segment's data is stored in an ensemble of bookies in
     * a stripping way. Each entry will be added in a <code>write-quorum</code>
     * size of bookies. The add operation will complete once it receives
     * responses from a <code>ack-quorum</code> size of bookies. The stripping
     * is done in a round-robin way in bookkeeper.
     * <p>
     * For example, we configure the ensemble-size to 5, write-quorum-size to 3,
     * and ack-quorum-size to 2. The data will be stored in following stripping way.
     * <pre>
     * | entry id | bk1 | bk2 | bk3 | bk4 | bk5 |
     * |     0    |  x  |  x  |  x  |     |     |
     * |     1    |     |  x  |  x  |  x  |     |
     * |     2    |     |     |  x  |  x  |  x  |
     * |     3    |  x  |     |     |  x  |  x  |
     * |     4    |  x  |  x  |     |     |  x  |
     * |     5    |  x  |  x  |  x  |     |     |
     * </pre>
     * <p>
     * We don't recommend stripping within a log segment to increase bandwidth.
     * We'd recommend to strip by `partition` in higher level of distributedlog
     * to increase performance. so typically the ensemble size will set to be
     * the same value as write quorum size.
     *
     * @return ensemble size
     * @see #getWriteQuorumSize()
     * @see #getAckQuorumSize()
     */
    public int getEnsembleSize() {
        return this.getInt(BKDL_BOOKKEEPER_ENSEMBLE_SIZE,
                getInt(BKDL_BOOKKEEPER_ENSEMBLE_SIZE_OLD,
                        BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT));
    }

    /**
     * Set ensemble size of each log segment (ledger) will use.
     *
     * @param ensembleSize ensemble size.
     * @return distributed log configuration
     * @see #getEnsembleSize()
     */
    public DistributedLogConfiguration setEnsembleSize(int ensembleSize) {
        setProperty(BKDL_BOOKKEEPER_ENSEMBLE_SIZE, ensembleSize);
        return this;
    }

    /**
     * Get write quorum size of each log segment (ledger) will use.
     * By default it is 3.
     *
     * @return write quorum size
     * @see #getEnsembleSize()
     */
    public int getWriteQuorumSize() {
        return this.getInt(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE,
                getInt(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_OLD,
                        BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT));
    }

    /**
     * Set write quorum size of each log segment (ledger) will use.
     *
     * @param quorumSize
     *          quorum size.
     * @return distributedlog configuration.
     * @see #getWriteQuorumSize()
     */
    public DistributedLogConfiguration setWriteQuorumSize(int quorumSize) {
        setProperty(BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE, quorumSize);
        return this;
    }

    /**
     * Get ack quorum size of each log segment (ledger) will use.
     * By default it is 2.
     *
     * @return ack quorum size
     * @see #getEnsembleSize()
     */
    public int getAckQuorumSize() {
        return this.getInt(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE,
                getInt(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_OLD,
                        BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT));
    }

    /**
     * Set ack quorum size of each log segment (ledger) will use.
     *
     * @param quorumSize
     *          quorum size.
     * @return distributedlog configuration.
     * @see #getAckQuorumSize()
     */
    public DistributedLogConfiguration setAckQuorumSize(int quorumSize) {
        setProperty(BKDL_BOOKKEEPER_ACK_QUORUM_SIZE, quorumSize);
        return this;
    }

    /**
     * Get the quorum config for each log segment (ledger).
     *
     * @return quorum config that used by log segments
     * @see #getEnsembleSize()
     * @see #getWriteQuorumSize()
     * @see #getAckQuorumSize()
     */
    public QuorumConfig getQuorumConfig() {
        return new QuorumConfig(
                getEnsembleSize(),
                getWriteQuorumSize(),
                getAckQuorumSize());
    }

    /**
     * Get if row aware ensemble placement is enabled.
     * <p>If enabled, {@link DNSResolverForRows} will be used for dns resolution
     * rather than {@link DNSResolverForRacks}, if no other dns resolver set via
     * {@link #setEnsemblePlacementDnsResolverClass(Class)}.
     * By default it is disable.
     *
     * @return true if row aware ensemble placement is enabled, otherwise false.
     * @see #getEnsemblePlacementDnsResolverClass()
     */
    public boolean getRowAwareEnsemblePlacementEnabled() {
        return getBoolean(BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT,
                getBoolean(BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT_OLD,
                        BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT_DEFAULT));
    }

    /**
     * Set if we should enable row aware ensemble placement.
     *
     * @param enableRowAwareEnsemblePlacement
     *          enableRowAwareEnsemblePlacement
     * @return distributedlog configuration.
     * @see #getRowAwareEnsemblePlacementEnabled()
     */
    public DistributedLogConfiguration setRowAwareEnsemblePlacementEnabled(boolean enableRowAwareEnsemblePlacement) {
        setProperty(BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT, enableRowAwareEnsemblePlacement);
        return this;
    }

    /**
     * Get the DNS resolver class for bookkeeper ensemble placement.
     * <p>By default, {@link DNSResolverForRacks} will be used if
     * {@link #getRowAwareEnsemblePlacementEnabled()} is disabled and
     * {@link DNSResolverForRows} will be used if {@link #getRowAwareEnsemblePlacementEnabled()}
     * is enabled.
     *
     * @return dns resolver class for bookkeeper ensemble placement.
     * @throws ConfigurationException
     * @see #getRowAwareEnsemblePlacementEnabled()
     */
    public Class<? extends DNSToSwitchMapping> getEnsemblePlacementDnsResolverClass()
            throws ConfigurationException {
        Class<? extends DNSToSwitchMapping> defaultResolverCls;
        if (getRowAwareEnsemblePlacementEnabled()) {
            defaultResolverCls = DNSResolverForRows.class;
        } else {
            defaultResolverCls = DNSResolverForRacks.class;
        }
        return ReflectionUtils.getClass(this, BKDL_ENSEMBLE_PLACEMENT_DNS_RESOLVER_CLASS,
                defaultResolverCls, DNSToSwitchMapping.class, defaultLoader);
    }

    /**
     * Set the DNS resolver class for bookkeeper ensemble placement.
     *
     * @param dnsResolverClass
     *          dns resolver class for bookkeeper ensemble placement.
     * @return distributedlog configuration
     * @see #getEnsemblePlacementDnsResolverClass()
     */
    public DistributedLogConfiguration setEnsemblePlacementDnsResolverClass(
            Class<? extends DNSToSwitchMapping> dnsResolverClass) {
        setProperty(BKDL_ENSEMBLE_PLACEMENT_DNS_RESOLVER_CLASS, dnsResolverClass.getName());
        return this;
    }

    /**
     * Get mapping used to override the region mapping derived by the default resolver.
     * <p>It is a string of pairs of host-region mappings (host:region) separated by semicolon.
     * By default it is empty string.
     *
     * @return dns resolver overrides.
     * @see #getEnsemblePlacementDnsResolverClass()
     * @see DNSResolverForRacks
     * @see DNSResolverForRows
     */
    public String getBkDNSResolverOverrides() {
        return getString(BKDL_BK_DNS_RESOLVER_OVERRIDES, BKDL_BK_DNS_RESOLVER_OVERRIDES_DEFAULT);
    }

    /**
     * Set mapping used to override the region mapping derived by the default resolver
     * <p>It is a string of pairs of host-region mappings (host:region) separated by semicolon.
     * By default it is empty string.
     *
     * @param overrides
     *          dns resolver overrides
     * @return dl configuration.
     * @see #getBkDNSResolverOverrides()
     */
    public DistributedLogConfiguration setBkDNSResolverOverrides(String overrides) {
        setProperty(BKDL_BK_DNS_RESOLVER_OVERRIDES, overrides);
        return this;
    }

    //
    // BookKeeper General Settings
    //

    /**
     * Set password used by bookkeeper client for digestion.
     * <p>
     * NOTE: not recommend to change. will be derepcated in future.
     *
     * @param bkDigestPW BK password digest
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setBKDigestPW(String bkDigestPW) {
        setProperty(BKDL_BOOKKEEPER_DIGEST_PW, bkDigestPW);
        return this;
    }

    /**
     * Get password used by bookkeeper client for digestion.
     * <p>
     * NOTE: not recommend to change. will be deprecated in future.
     *
     * @return password used by bookkeeper client for digestion
     * @see #setBKDigestPW(String)
     */
    public String getBKDigestPW() {
        return getString(BKDL_BOOKKEEPER_DIGEST_PW, BKDL_BOOKKEEPER_DIGEST_PW_DEFAULT);
    }

    /**
     * Get BK client number of i/o threads used by Netty.
     * The default value equals DL's number worker threads.
     *
     * @return number of bookkeeper netty i/o threads.
     * @see #getNumWorkerThreads()
     */
    public int getBKClientNumberIOThreads() {
        return this.getInt(BKDL_BKCLIENT_NUM_IO_THREADS, getNumWorkerThreads());
    }

    /**
     * Set BK client number of i/o threads used by netty.
     *
     * @param numThreads
     *          number io threads.
     * @return distributedlog configuration.
     * @see #getBKClientNumberIOThreads()
     */
    public DistributedLogConfiguration setBKClientNumberIOThreads(int numThreads) {
        setProperty(BKDL_BKCLIENT_NUM_IO_THREADS, numThreads);
        return this;
    }

    /**
     * Get the tick duration in milliseconds that used for timeout timer in bookkeeper client.
     * By default it is 100.
     *
     * @return tick duration in milliseconds
     * @see org.jboss.netty.util.HashedWheelTimer
     */
    public long getTimeoutTimerTickDurationMs() {
        return getLong(BKDL_TIMEOUT_TIMER_TICK_DURATION_MS, BKDL_TIMEOUT_TIMER_TICK_DURATION_MS_DEFAULT);
    }

    /**
     * Set the tick duration in milliseconds that used for timeout timer in bookkeeper client.
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return distributed log configuration.
     * @see #getTimeoutTimerTickDurationMs()
     */
    public DistributedLogConfiguration setTimeoutTimerTickDurationMs(long tickDuration) {
        setProperty(BKDL_TIMEOUT_TIMER_TICK_DURATION_MS, tickDuration);
        return this;
    }

    /**
     * Get number of ticks that used for timeout timer in bookkeeper client.
     * By default is 1024.
     *
     * @return number of ticks that used for timeout timer.
     * @see org.jboss.netty.util.HashedWheelTimer
     */
    public int getTimeoutTimerNumTicks() {
        return getInt(BKDL_TIMEOUT_TIMER_NUM_TICKS, BKDL_TIMEOUT_TIMER_NUM_TICKS_DEFAULT);
    }

    /**
     * Set number of ticks that used for timeout timer in bookkeeper client.
     *
     * @param numTicks
     *          number of ticks that used for timeout timer.
     * @return distributed log configuration.
     * @see #getTimeoutTimerNumTicks()
     */
    public DistributedLogConfiguration setTimeoutTimerNumTicks(int numTicks) {
        setProperty(BKDL_TIMEOUT_TIMER_NUM_TICKS, numTicks);
        return this;
    }

    //
    // Deprecated BookKeeper Settings
    //

    /**
     * Get BK client read timeout in seconds.
     * <p>
     * Please use {@link ClientConfiguration#getReadEntryTimeout()}
     * instead of this setting.
     *
     * @return read timeout in seconds
     * @deprecated
     * @see ClientConfiguration#getReadEntryTimeout()
     */
    public int getBKClientReadTimeout() {
        return this.getInt(BKDL_BKCLIENT_READ_TIMEOUT,
                BKDL_BKCLIENT_READ_TIMEOUT_DEFAULT);
    }

    /**
     * Set BK client read timeout in seconds.
     *
     * @param readTimeout read timeout in seconds.
     * @return distributed log configuration
     * @deprecated
     * @see #getBKClientReadTimeout()
     */
    public DistributedLogConfiguration setBKClientReadTimeout(int readTimeout) {
        setProperty(BKDL_BKCLIENT_READ_TIMEOUT, readTimeout);
        return this;
    }

    /**
     * Get BK client write timeout in seconds.
     * <p>
     * Please use {@link ClientConfiguration#getAddEntryTimeout()}
     * instead of this setting.
     *
     * @return write timeout in seconds.
     * @deprecated
     * @see ClientConfiguration#getAddEntryTimeout()
     */
    public int getBKClientWriteTimeout() {
        return this.getInt(BKDL_BKCLIENT_WRITE_TIMEOUT, BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);
    }

    /**
     * Set BK client write timeout in seconds
     *
     * @param writeTimeout write timeout in seconds.
     * @return distributed log configuration
     * @deprecated
     * @see #getBKClientWriteTimeout()
     */
    public DistributedLogConfiguration setBKClientWriteTimeout(int writeTimeout) {
        setProperty(BKDL_BKCLIENT_WRITE_TIMEOUT, writeTimeout);
        return this;
    }

    /**
     * Get BK client number of worker threads.
     * <p>
     * Please use {@link ClientConfiguration#getNumWorkerThreads()}
     * instead of this setting.
     *
     * @return number of bookkeeper client worker threads.
     * @deprecated
     * @see ClientConfiguration#getNumWorkerThreads()
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
     * @deprecated
     * @see #getBKClientNumberWorkerThreads()
     */
    public DistributedLogConfiguration setBKClientNumberWorkerThreads(int numThreads) {
        setProperty(BKDL_BKCLIENT_NUM_WORKER_THREADS, numThreads);
        return this;
    }

    //
    // DL Executor Settings
    //

    /**
     * Get the number of worker threads used by distributedlog namespace.
     * By default it is the number of available processors.
     *
     * @return number of worker threads used by distributedlog namespace.
     */
    public int getNumWorkerThreads() {
        return getInt(BKDL_NUM_WORKER_THREADS, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Set the number of worker threads used by distributedlog namespace.
     *
     * @param numWorkerThreads
     *          number of worker threads used by distributedlog namespace.
     * @return configuration
     * @see #getNumWorkerThreads()
     */
    public DistributedLogConfiguration setNumWorkerThreads(int numWorkerThreads) {
        setProperty(BKDL_NUM_WORKER_THREADS, numWorkerThreads);
        return this;
    }

    /**
     * Get the number of dedicated readahead worker threads used by distributedlog namespace.
     * <p>If this value is non-positive, it would share the normal executor (see {@link #getNumWorkerThreads()}
     * for readahead. otherwise, it would use a dedicated executor for readhead. By default,
     * it is 0.
     *
     * @return number of dedicated readahead worker threads.
     * @see #getNumWorkerThreads()
     */
    public int getNumReadAheadWorkerThreads() {
        return getInt(BKDL_NUM_READAHEAD_WORKER_THREADS, 0);
    }

    /**
     * Set the number of dedicated readahead worker threads used by distributedlog namespace.
     *
     * @param numWorkerThreads
     *          number of dedicated readahead worker threads.
     * @return configuration
     * @see #getNumReadAheadWorkerThreads()
     */
    public DistributedLogConfiguration setNumReadAheadWorkerThreads(int numWorkerThreads) {
        setProperty(BKDL_NUM_READAHEAD_WORKER_THREADS, numWorkerThreads);
        return this;
    }

    /**
     * Get the number of lock state threads used by distributedlog namespace.
     * By default it is 1.
     *
     * @return number of lock state threads used by distributedlog namespace.
     */
    public int getNumLockStateThreads() {
        return getInt(BKDL_NUM_LOCKSTATE_THREADS, 1);
    }

    /**
     * Set the number of lock state threads used by distributedlog manager factory.
     *
     * @param numLockStateThreads
     *          number of lock state threads used by distributedlog manager factory.
     * @return configuration
     * @see #getNumLockStateThreads()
     */
    public DistributedLogConfiguration setNumLockStateThreads(int numLockStateThreads) {
        setProperty(BKDL_NUM_LOCKSTATE_THREADS, numLockStateThreads);
        return this;
    }

    /**
     * Get timeout for shutting down schedulers in dl manager, in milliseconds.
     * By default, it is 5 seconds.
     *
     * @return timeout for shutting down schedulers in dl manager, in miliseconds.
     */
    public int getSchedulerShutdownTimeoutMs() {
        return getInt(BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS, BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS_DEFAULT);
    }

    /**
     * Set timeout for shutting down schedulers in dl manager, in milliseconds.
     *
     * @param timeoutMs
     *         timeout for shutting down schedulers in dl manager, in milliseconds.
     * @return dl configuration.
     * @see #getSchedulerShutdownTimeoutMs()
     */
    public DistributedLogConfiguration setSchedulerShutdownTimeoutMs(int timeoutMs) {
        setProperty(BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS, timeoutMs);
        return this;
    }

    /**
     * Whether to use daemon thread for DL threads.
     * By default it is false.
     *
     * @return true if use daemon threads, otherwise false.
     */
    public boolean getUseDaemonThread() {
        return getBoolean(BKDL_USE_DAEMON_THREAD, BKDL_USE_DAEMON_THREAD_DEFAULT);
    }

    /**
     * Set whether to use daemon thread for DL threads.
     *
     * @param daemon
     *          whether to use daemon thread for DL threads.
     * @return distributedlog configuration
     * @see #getUseDaemonThread()
     */
    public DistributedLogConfiguration setUseDaemonThread(boolean daemon) {
        setProperty(BKDL_USE_DAEMON_THREAD, daemon);
        return this;
    }

    //
    // Metadata Settings
    //

    /**
     * Get DL ledger metadata output layout version.
     *
     * @return layout version
     * @see com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion
     */
    public int getDLLedgerMetadataLayoutVersion() {
        return this.getInt(BKDL_LEDGER_METADATA_LAYOUT_VERSION,
                getInt(BKDL_LEDGER_METADATA_LAYOUT_VERSION_OLD,
                        BKDL_LEDGER_METADATA_LAYOUT_VERSION_DEFAULT));
    }

    /**
     * Set DL ledger metadata output layout version.
     *
     * @param layoutVersion layout version
     * @return distributed log configuration
     * @throws IllegalArgumentException if setting an unknown layout version.
     * @see #getDLLedgerMetadataLayoutVersion()
     */
    public DistributedLogConfiguration setDLLedgerMetadataLayoutVersion(int layoutVersion)
            throws IllegalArgumentException {
        if ((layoutVersion <= 0) ||
            (layoutVersion > LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION)) {
            // Incorrect version specified
            throw new IllegalArgumentException("Incorrect value for ledger metadata layout version");
        }
        setProperty(BKDL_LEDGER_METADATA_LAYOUT_VERSION, layoutVersion);
        return this;
    }

    /**
     * Get the setting for whether we should enforce the min ledger metadata version check.
     * By default it is false.
     *
     * @return whether we should enforce the min ledger metadata version check
     * @see com.twitter.distributedlog.LogSegmentMetadata.LogSegmentMetadataVersion
     */
    public boolean getDLLedgerMetadataSkipMinVersionCheck() {
        return this.getBoolean(BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK,
                BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK_DEFAULT);
    }

    /**
     * Set if we should skip the enforcement of min ledger metadata version.
     * <p>NOTE: please be aware the side effects of skipping min ledger metadata
     * version checking.
     *
     * @param skipMinVersionCheck whether we should enforce the min ledger metadata version check
     * @return distributed log configuration
     * @see #getDLLedgerMetadataSkipMinVersionCheck()
     */
    public DistributedLogConfiguration setDLLedgerMetadataSkipMinVersionCheck(boolean skipMinVersionCheck) throws IllegalArgumentException {
        setProperty(BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK, skipMinVersionCheck);
        return this;
    }

    /**
     * Get the value at which ledger sequence number should start for streams that are being
     * upgraded and did not have ledger sequence number to start with or for newly created
     * streams. By default, it is 1.
     * <p>In most of the cases this value should not be changed. It is useful for backfilling
     * in the case of migrating log segments whose metadata don't have log segment sequence number.
     *
     * @return first ledger sequence number
     */
    public long getFirstLogSegmentSequenceNumber() {
        return this.getLong(BKDL_FIRST_LOGSEGMENT_SEQUENCE_NUMBER,
                getLong(BKDL_FIRST_LOGSEGMENT_SEQUENCE_NUMBER_OLD,
                        BKDL_FIRST_LOGSEGMENT_SEQUENCE_NUMBER_DEFAULT));
    }

    /**
     * Set the value at which ledger sequence number should start for streams that are being
     * upgraded and did not have ledger sequence number to start with or for newly created
     * streams
     *
     * @param firstLogSegmentSequenceNumber first ledger sequence number
     * @return distributed log configuration
     * @see #getFirstLogSegmentSequenceNumber()
     */
    public DistributedLogConfiguration setFirstLogSegmentSequenceNumber(long firstLogSegmentSequenceNumber)
            throws IllegalArgumentException {
        if (firstLogSegmentSequenceNumber <= 0) {
            // Incorrect ledger sequence number specified
            throw new IllegalArgumentException("Incorrect value for ledger sequence number");
        }
        setProperty(BKDL_FIRST_LOGSEGMENT_SEQUENCE_NUMBER, firstLogSegmentSequenceNumber);
        return this;
    }

    /**
     * Whether we should publish record counts in the log records and metadata.
     * <p>By default it is true. This is a legacy setting for log segment version 1. It
     * should be considered removed.
     *
     * @return if record counts should be persisted
     */
    public boolean getEnableRecordCounts() {
        return getBoolean(BKDL_ENABLE_RECORD_COUNTS, BKDL_ENABLE_RECORD_COUNTS_DEFAULT);
    }

    /**
     * Set if we should publish record counts in the log records and metadata.
     *
     * @param enableRecordCounts enable record counts
     * @return distributed log configuration
     * @see #getEnableRecordCounts()
     */
    public DistributedLogConfiguration setEnableRecordCounts(boolean enableRecordCounts) {
        setProperty(BKDL_ENABLE_RECORD_COUNTS, enableRecordCounts);
        return this;
    }

    /**
     * Whether sanity check txn id on starting log segments.
     * <p>If it is enabled, DL writer would throw
     * {@link com.twitter.distributedlog.exceptions.TransactionIdOutOfOrderException}
     * when it received a smaller transaction id than current maximum transaction id.
     *
     * @return true if should check txn id with max txn id, otherwise false.
     */
    public boolean getSanityCheckTxnID() {
        return getBoolean(BKDL_MAXID_SANITYCHECK, BKDL_MAXID_SANITYCHECK_DEFAULT);
    }

    /**
     * Enable/Disable sanity check txn id.
     *
     * @param enabled
     *          enable/disable sanity check txn id.
     * @return configuration.
     * @see #getSanityCheckTxnID()
     */
    public DistributedLogConfiguration setSanityCheckTxnID(boolean enabled) {
        setProperty(BKDL_MAXID_SANITYCHECK, enabled);
        return this;
    }

    /**
     * Whether encode region id in log segment metadata.
     * <p>In global DL use case, encoding region id in log segment medata would
     * help understanding what region that a log segment is created. The region
     * id field in log segment metadata would help for moniotring and troubleshooting.
     *
     * @return whether to encode region id in log segment metadata.
     */
    public boolean getEncodeRegionIDInLogSegmentMetadata() {
        return getBoolean(BKDL_ENCODE_REGION_ID_IN_VERSION, BKDL_ENCODE_REGION_ID_IN_VERSION_DEFAULT);
    }

    /**
     * Enable/Disable encoding region id in log segment metadata.
     *
     * @param enabled
     *          flag to enable/disable encoding region id in log segment metadata.
     * @return configuration instance.
     * @see #getEncodeRegionIDInLogSegmentMetadata()
     */
    public DistributedLogConfiguration setEncodeRegionIDInLogSegmentMetadata(boolean enabled) {
        setProperty(BKDL_ENCODE_REGION_ID_IN_VERSION, enabled);
        return this;
    }

    /**
     * Get log segment name version.
     * <p>
     * <ul>
     * <li>version 0: inprogress_(start_txid) |
     * logrecs_(start_txid)_(end_txid)</li>
     * <li>version 1: inprogress_(logsegment_sequence_number) |
     * logrecs_(logsegment_sequence_number)</li>
     * </ul>
     * By default it is 1.
     *
     * @return log segment name verison.
     */
    public int getLogSegmentNameVersion() {
        return getInt(BKDL_LOGSEGMENT_NAME_VERSION, BKDL_LOGSEGMENT_NAME_VERSION_DEFAULT);
    }

    /**
     * Set log segment name version.
     *
     * @param version
     *          log segment name version.
     * @return configuration object.
     * @see #getLogSegmentNameVersion()
     */
    public DistributedLogConfiguration setLogSegmentNameVersion(int version) {
        setProperty(BKDL_LOGSEGMENT_NAME_VERSION, version);
        return this;
    }

    /**
     * Get name of the unpartitioned stream.
     * <p>It is a legacy setting. consider removing it in future.
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
     * @return distributedlog configuration
     * @see #getUnpartitionedStreamName()
     */
    public DistributedLogConfiguration setUnpartitionedStreamName(String streamName) {
        setProperty(BKDL_UNPARTITIONED_STREAM_NAME, streamName);
        return this;
    }

    //
    // DL Writer General Settings
    //

    /**
     * Whether to create stream if not exists. By default it is true.
     *
     * @return true if it is abled to create stream if not exists.
     */
    public boolean getCreateStreamIfNotExists() {
        return getBoolean(BKDL_CREATE_STREAM_IF_NOT_EXISTS,
                BKDL_CREATE_STREAM_IF_NOT_EXISTS_DEFAULT);
    }

    /**
     * Enable/Disable creating stream if not exists.
     *
     * @param enabled
     *          enable/disable sanity check txn id.
     * @return distributed log configuration.
     * @see #getCreateStreamIfNotExists()
     */
    public DistributedLogConfiguration setCreateStreamIfNotExists(boolean enabled) {
        setProperty(BKDL_CREATE_STREAM_IF_NOT_EXISTS, enabled);
        return this;
    }

    /**
     * Get Log Flush timeout in seconds.
     * <p>This is a setting used by DL writer on flushing data. It is typically used
     * by synchronous writer and log segment writer. By default it is 30 seconds.
     *
     * @return log flush timeout in seconds.
     */
    // @Deprecated
    public int getLogFlushTimeoutSeconds() {
        return this.getInt(BKDL_LOG_FLUSH_TIMEOUT, BKDL_LOG_FLUSH_TIMEOUT_DEFAULT);
    }

    /**
     * Set Log Flush Timeout in seconds.
     *
     * @param logFlushTimeoutSeconds log flush timeout.
     * @return distributed log configuration
     * @see #getLogFlushTimeoutSeconds()
     */
    public DistributedLogConfiguration setLogFlushTimeoutSeconds(int logFlushTimeoutSeconds) {
        setProperty(BKDL_LOG_FLUSH_TIMEOUT, logFlushTimeoutSeconds);
        return this;
    }

    /**
     * The compression type to use while sending data to bookkeeper.
     *
     * @return compression type to use
     * @see com.twitter.distributedlog.util.CompressionCodec
     */
    public String getCompressionType() {
        return getString(BKDL_COMPRESSION_TYPE, BKDL_COMPRESSION_TYPE_DEFAULT);
    }

    /**
     * Set the compression type to use while sending data to bookkeeper.
     *
     * @param compressionType compression type
     * @return distributedlog configuration
     * @see #getCompressionType()
     */
    public DistributedLogConfiguration setCompressionType(String compressionType) {
        Preconditions.checkArgument(null != compressionType && !compressionType.isEmpty());
        setProperty(BKDL_COMPRESSION_TYPE, compressionType);
        return this;
    }

    /**
     * Whether to fail immediately if the stream is not ready rather than queueing the request.
     * <p>If it is enabled, it would fail the write request immediately if the stream isn't ready.
     * Consider turning it on for the use cases that could retry writing to other streams
     * (aka non-strict ordering guarantee). It would result fast failure hence the client would
     * retry immediately.
     *
     * @return true if should fail fast. otherwise, false.
     */
    public boolean getFailFastOnStreamNotReady() {
        return getBoolean(BKDL_FAILFAST_ON_STREAM_NOT_READY,
                BKDL_FAILFAST_ON_STREAM_NOT_READY_DEFAULT);
    }

    /**
     * Set the failfast on stream not ready flag.
     *
     * @param failFastOnStreamNotReady
     *        set failfast flag
     * @return dl configuration.
     * @see #getFailFastOnStreamNotReady()
     */
    public DistributedLogConfiguration setFailFastOnStreamNotReady(boolean failFastOnStreamNotReady) {
        setProperty(BKDL_FAILFAST_ON_STREAM_NOT_READY, failFastOnStreamNotReady);
        return this;
    }

    /**
     * If this option is set, the log writer won't reset the segment writer if an error
     * is encountered.
     *
     * @return true if we should disable automatic rolling
     */
    public boolean getDisableRollingOnLogSegmentError() {
        return getBoolean(BKDL_DISABLE_ROLLING_ON_LOG_SEGMENT_ERROR,
                BKDL_DISABLE_ROLLING_ON_LOG_SEGMENT_ERROR_DEFAULT);
    }

    /**
     * Set the roll on segment error flag.
     *
     * @param disableRollingOnLogSegmentError
     *        set roll on error flag
     * @return dl configuration.
     * @see #getDisableRollingOnLogSegmentError()
     */
    public DistributedLogConfiguration setDisableRollingOnLogSegmentError(boolean disableRollingOnLogSegmentError) {
        setProperty(BKDL_DISABLE_ROLLING_ON_LOG_SEGMENT_ERROR, disableRollingOnLogSegmentError);
        return this;
    }

    //
    // DL Durability Settings
    //

    /**
     * Check whether the durable write is enabled.
     * <p>It is enabled by default.
     *
     * @return true if durable write is enabled. otherwise, false.
     */
    public boolean isDurableWriteEnabled() {
        return this.getBoolean(BKDL_IS_DURABLE_WRITE_ENABLED, BKDL_IS_DURABLE_WRITE_ENABLED_DEFAULT);
    }

    /**
     * Enable/Disable durable writes in writers.
     *
     * @param enabled
     *          flag to enable/disable durable writes in writers.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setDurableWriteEnabled(boolean enabled) {
        setProperty(BKDL_IS_DURABLE_WRITE_ENABLED, enabled);
        return this;
    }

    //
    // DL Writer Transmit Settings
    //

    /**
     * Get output buffer size for DL writers, in bytes.
     * <p>Large buffer will result in higher compression ratio and
     * it would use the bandwidth more efficiently and improve throughput.
     * Set it to 0 would ask DL writers to transmit the data immediately,
     * which it could achieve low latency.
     * <p>The default value is 1KB.
     *
     * @return buffer size in byes.
     */
    public int getOutputBufferSize() {
        return this.getInt(BKDL_OUTPUT_BUFFER_SIZE,
                getInt(BKDL_OUTPUT_BUFFER_SIZE_OLD, BKDL_OUTPUT_BUFFER_SIZE_DEFAULT));
    }

    /**
     * Set output buffer size for DL writers, in bytes.
     *
     * @param opBufferSize output buffer size.
     * @return distributed log configuration
     * @see #getOutputBufferSize()
     */
    public DistributedLogConfiguration setOutputBufferSize(int opBufferSize) {
        setProperty(BKDL_OUTPUT_BUFFER_SIZE, opBufferSize);
        return this;
    }

    /**
     * Get Periodic Log Flush Frequency in milliseconds.
     * <p>If the setting is set with a positive value, the data in output buffer
     * will be flushed in this provided interval. The default value is 0.
     *
     * @return periodic flush frequency in milliseconds.
     * @see #getOutputBufferSize()
     */
    public int getPeriodicFlushFrequencyMilliSeconds() {
        return this.getInt(BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS,
                BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT);
    }

    /**
     * Set Periodic Log Flush Frequency in milliseconds.
     *
     * @param flushFrequencyMs periodic flush frequency in milliseconds.
     * @return distributed log configuration
     * @see #getPeriodicFlushFrequencyMilliSeconds()
     */
    public DistributedLogConfiguration setPeriodicFlushFrequencyMilliSeconds(int flushFrequencyMs) {
        setProperty(BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS, flushFrequencyMs);
        return this;
    }

    /**
     * Is immediate flush enabled.
     * <p>If it is enabled, it would flush control record immediately after adding
     * data completed. The default value is false.
     *
     * @return whether immediate flush is enabled
     */
    public boolean getImmediateFlushEnabled() {
        return getBoolean(BKDL_ENABLE_IMMEDIATE_FLUSH, BKDL_ENABLE_IMMEDIATE_FLUSH_DEFAULT);
    }

    /**
     * Enable/Disable immediate flush
     *
     * @param enabled
     *          flag to enable/disable immediate flush.
     * @return configuration instance.
     * @see #getImmediateFlushEnabled()
     */
    public DistributedLogConfiguration setImmediateFlushEnabled(boolean enabled) {
        setProperty(BKDL_ENABLE_IMMEDIATE_FLUSH, enabled);
        return this;
    }

    /**
     * Get minimum delay between immediate flushes in milliseconds.
     * <p>This setting only takes effects when {@link #getImmediateFlushEnabled()}
     * is enabled. It torelants the bursty of traffic when immediate flush is enabled,
     * which prevents sending too many control records to the bookkeeper.
     *
     * @return minimum delay between immediate flushes in milliseconds
     * @see #getImmediateFlushEnabled()
     */
    public int getMinDelayBetweenImmediateFlushMs() {
        return this.getInt(BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS, BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS_DEFAULT);
    }

    /**
     * Set minimum delay between immediate flushes in milliseconds
     *
     * @param minDelayMs minimum delay between immediate flushes in milliseconds.
     * @return distributed log configuration
     * @see #getMinDelayBetweenImmediateFlushMs()
     */
    public DistributedLogConfiguration setMinDelayBetweenImmediateFlushMs(int minDelayMs) {
        setProperty(BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS, minDelayMs);
        return this;
    }

    //
    // DL Retention/Truncation Settings
    //

    /**
     * Get log segment retention period in hours.
     * The default value is 3 days.
     *
     * @return log segment retention period in hours
     */
    public int getRetentionPeriodHours() {
        return this.getInt(BKDL_RETENTION_PERIOD_IN_HOURS,
                getInt(BKDL_RETENTION_PERIOD_IN_HOURS_OLD,
                        BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT));
    }

    /**
     * Set log segment retention period in hours.
     *
     * @param retentionHours retention period in hours.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setRetentionPeriodHours(int retentionHours) {
        setProperty(BKDL_RETENTION_PERIOD_IN_HOURS, retentionHours);
        return this;
    }

    /**
     * Is truncation managed explicitly by the application.
     * <p>If this is set then time based retention is only a hint to perform
     * deferred cleanup. However we never remove a segment that has not been
     * already marked truncated.
     * <p>It is disabled by default.
     *
     * @return whether truncation managed explicitly by the application
     * @see com.twitter.distributedlog.LogSegmentMetadata.TruncationStatus
     */
    public boolean getExplicitTruncationByApplication() {
        return getBoolean(BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION,
                BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION_DEFAULT);
    }

    /**
     * Enable/Disable whether truncation is managed explicitly by the application.
     *
     * @param enabled
     *          flag to enable/disable whether truncation is managed explicitly by the application.
     * @return configuration instance.
     */
    public DistributedLogConfiguration setExplicitTruncationByApplication(boolean enabled) {
        setProperty(BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION, enabled);
        return this;
    }

    //
    // Log Segment Rolling Settings
    //

    /**
     * Get log segment rolling interval in minutes.
     * <p>If the setting is set to a positive value, DL writer will roll log segments
     * based on time. Otherwise, it will roll log segments based on size.
     * <p>The default value is 2 hours.
     *
     * @return log segment rolling interval in minutes
     * @see #getMaxLogSegmentBytes()
     */
    public int getLogSegmentRollingIntervalMinutes() {
        return this.getInt(BKDL_ROLLING_INTERVAL_IN_MINUTES,
                getInt(BKDL_ROLLING_INTERVAL_IN_MINUTES_OLD,
                        BKDL_ROLLING_INTERVAL_IN_MINUTES_DEFAULT));
    }

    /**
     * Set log segment rolling interval in minutes.
     *
     * @param rollingMinutes rolling interval in minutes.
     * @return distributed log configuration
     * @see #getLogSegmentRollingIntervalMinutes()
     */
    public DistributedLogConfiguration setLogSegmentRollingIntervalMinutes(int rollingMinutes) {
        setProperty(BKDL_ROLLING_INTERVAL_IN_MINUTES, rollingMinutes);
        return this;
    }

    /**
     * Get Max LogSegment Size in Bytes.
     * <p>This setting only takes effects when time based rolling is disabled.
     * DL writer will roll into a new log segment only after current one reaches
     * this threshold.
     * <p>The default value is 256MB.
     *
     * @return max logsegment size in bytes.
     * @see #getLogSegmentRollingIntervalMinutes()
     */
    public long getMaxLogSegmentBytes() {
        long maxBytes = this.getLong(BKDL_MAX_LOGSEGMENT_BYTES, BKDL_MAX_LOGSEGMENT_BYTES_DEFAULT);
        if (maxBytes <= 0) {
            maxBytes = BKDL_MAX_LOGSEGMENT_BYTES_DEFAULT;
        }
        return maxBytes;
    }

    /**
     * Set Max LogSegment Size in Bytes.
     *
     * @param maxBytes
     *          max logsegment size in bytes.
     * @return configuration.
     * @see #getMaxLogSegmentBytes()
     */
    public DistributedLogConfiguration setMaxLogSegmentBytes(long maxBytes) {
        setProperty(BKDL_MAX_LOGSEGMENT_BYTES, maxBytes);
        return this;
    }

    /**
     * Get log segment rolling concurrency.
     * <p>It limits how many writers could roll log segments concurrently.
     * The default value is 1.
     *
     * @return log segment rolling concurrency.
     * @see #setLogSegmentRollingConcurrency(int)
     */
    public int getLogSegmentRollingConcurrency() {
        return getInt(BKDL_LOGSEGMENT_ROLLING_CONCURRENCY, BKDL_LOGSEGMENT_ROLLING_CONCURRENCY_DEFAULT);
    }

    /**
     * Set log segment rolling concurrency. <i>0</i> means disable rolling concurrency.
     * <i>larger than 0</i> means how many log segment could be rolled at the same time.
     * <i>less than 0</i> means unlimited concurrency on rolling log segments.
     *
     * @param concurrency
     *          log segment rolling concurrency.
     * @return distributed log configuration.
     * @see #getLogSegmentRollingConcurrency()
     */
    public DistributedLogConfiguration setLogSegmentRollingConcurrency(int concurrency) {
        setProperty(BKDL_LOGSEGMENT_ROLLING_CONCURRENCY, concurrency);
        return this;
    }

    //
    // Lock Settings
    //

    /**
     * Get lock timeout in milliseconds. The default value is 30.
     *
     * @return lock timeout in milliseconds
     */
    public long getLockTimeoutMilliSeconds() {
        return this.getLong(BKDL_LOCK_TIMEOUT, BKDL_LOCK_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set lock timeout in seconds.
     *
     * @param lockTimeout lock timeout in seconds.
     * @return distributed log configuration
     * @see #getLockTimeoutMilliSeconds()
     */
    public DistributedLogConfiguration setLockTimeout(long lockTimeout) {
        setProperty(BKDL_LOCK_TIMEOUT, lockTimeout);
        return this;
    }

    /**
     * Get lock reacquire timeout in milliseconds. The default value is 120 seconds.
     *
     * @return lock reacquire timeout in milliseconds
     */
    public long getLockReacquireTimeoutMilliSeconds() {
        return this.getLong(BKDL_LOCK_REACQUIRE_TIMEOUT, BKDL_LOCK_REACQUIRE_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set lock reacquire timeout in seconds.
     *
     * @param lockReacquireTimeout lock reacquire timeout in seconds.
     * @return distributed log configuration
     * @see #getLockReacquireTimeoutMilliSeconds()
     */
    public DistributedLogConfiguration setLockReacquireTimeoutSeconds(long lockReacquireTimeout) {
        setProperty(BKDL_LOCK_REACQUIRE_TIMEOUT, lockReacquireTimeout);
        return this;
    }

    /**
     * Get lock internal operation timeout in milliseconds.
     * The default value is 120 seconds.
     *
     * @return lock internal operation timeout in milliseconds.
     */
    public long getLockOpTimeoutMilliSeconds() {
        return this.getLong(BKDL_LOCK_OP_TIMEOUT, BKDL_LOCK_OP_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set lock internal operation timeout in seconds.
     *
     * @param lockOpTimeout lock internal operation timeout in seconds.
     * @return distributed log configuration
     * @see #getLockOpTimeoutMilliSeconds()
     */
    public DistributedLogConfiguration setLockOpTimeoutSeconds(long lockOpTimeout) {
        setProperty(BKDL_LOCK_OP_TIMEOUT, lockOpTimeout);
        return this;
    }

    //
    // Ledger Allocator Settings
    //

    /**
     * Whether to enable ledger allocator pool or not.
     * It is disabled by default.
     *
     * @return whether using ledger allocator pool or not.
     */
    public boolean getEnableLedgerAllocatorPool() {
        return getBoolean(BKDL_ENABLE_LEDGER_ALLOCATOR_POOL, BKDL_ENABLE_LEDGER_ALLOCATOR_POOL_DEFAULT);
    }

    /**
     * Enable/Disable ledger allocator pool.
     *
     * @param enabled
     *          enable/disable ledger allocator pool.
     * @return configuration.
     * @see #getEnableLedgerAllocatorPool()
     */
    public DistributedLogConfiguration setEnableLedgerAllocatorPool(boolean enabled) {
        setProperty(BKDL_ENABLE_LEDGER_ALLOCATOR_POOL, enabled);
        return this;
    }

    /**
     * Get the path of ledger allocator pool.
     * The default value is ".allocation_pool".
     *
     * @return path of ledger allocator pool.
     */
    public String getLedgerAllocatorPoolPath() {
        return getString(BKDL_LEDGER_ALLOCATOR_POOL_PATH, BKDL_LEDGER_ALLOCATOR_POOL_PATH_DEFAULT);
    }

    /**
     * Set the root path of ledger allocator pool
     *
     * @param path
     *          path of ledger allocator pool.
     * @return configuration
     * @see #getLedgerAllocatorPoolPath()
     */
    public DistributedLogConfiguration setLedgerAllocatorPoolPath(String path) {
        setProperty(BKDL_LEDGER_ALLOCATOR_POOL_PATH, path);
        return this;
    }

    /**
     * Get the name of ledger allocator pool.
     *
     * @return name of ledger allocator pool.
     */
    public String getLedgerAllocatorPoolName() {
        return getString(BKDL_LEDGER_ALLOCATOR_POOL_NAME, BKDL_LEDGER_ALLOCATOR_POOL_NAME_DEFAULT);
    }

    /**
     * Set name of ledger allocator pool.
     *
     * @param name
     *          name of ledger allocator pool.
     * @return configuration.
     */
    public DistributedLogConfiguration setLedgerAllocatorPoolName(String name) {
        setProperty(BKDL_LEDGER_ALLOCATOR_POOL_NAME, name);
        return this;
    }

    /**
     * Get the core size of ledger allocator pool.
     * The default value is 20.
     *
     * @return core size of ledger allocator pool.
     */
    public int getLedgerAllocatorPoolCoreSize() {
        return getInt(BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE, BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE_DEFAULT);
    }

    /**
     * Set core size of ledger allocator pool.
     *
     * @param poolSize
     *          core size of ledger allocator pool.
     * @return distributedlog configuration.
     * @see #getLedgerAllocatorPoolCoreSize()
     */
    public DistributedLogConfiguration setLedgerAllocatorPoolCoreSize(int poolSize) {
        setProperty(BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE, poolSize);
        return this;
    }

    //
    // Write Limit Settings
    //

    /**
     * Get the per stream outstanding write limit for dl.
     * <p>If the setting is set with a positive value, the per stream
     * write limiting is enabled. By default it is disabled.
     *
     * @return the per stream outstanding write limit for dl
     * @see #getGlobalOutstandingWriteLimit()
     */
    public int getPerWriterOutstandingWriteLimit() {
        return getInt(BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT,
                BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT_DEFAULT);
    }

    /**
     * Set the per stream outstanding write limit for dl.
     *
     * @param limit
     *          per stream outstanding write limit for dl
     * @return dl configuration
     * @see #getPerWriterOutstandingWriteLimit()
     */
    public DistributedLogConfiguration setPerWriterOutstandingWriteLimit(int limit) {
        setProperty(BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT, limit);
        return this;
    }

    /**
     * Get the global write limit for dl.
     * <p>If the setting is set with a positive value, the global
     * write limiting is enabled. By default it is disabled.
     *
     * @return the global write limit for dl
     * @see #getPerWriterOutstandingWriteLimit()
     */
    public int getGlobalOutstandingWriteLimit() {
        return getInt(BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT, BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT_DEFAULT);
    }

    /**
     * Set the global write limit for dl.
     *
     * @param limit
     *          global write limit for dl
     * @return dl configuration
     * @see #getGlobalOutstandingWriteLimit()
     */
    public DistributedLogConfiguration setGlobalOutstandingWriteLimit(int limit) {
        setProperty(BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT, limit);
        return this;
    }

    /**
     * Whether to darkmode outstanding writes limit.
     * <p>If it is running in darkmode, it would not reject requests when
     * it is over limit, but just record them in the stats.
     * <p>By default, it is in darkmode.
     *
     * @return flag to darmkode pending write limit.
     */
    public boolean getOutstandingWriteLimitDarkmode() {
        return getBoolean(BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE,
                BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE_DEFAULT);
    }

    /**
     * Set the flag to darkmode outstanding writes limit.
     *
     * @param darkmoded
     *          flag to darmkode pending write limit
     * @return dl configuration.
     * @see #getOutstandingWriteLimitDarkmode()
     */
    public DistributedLogConfiguration setOutstandingWriteLimitDarkmode(boolean darkmoded) {
        setProperty(BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE, darkmoded);
        return this;
    }

    //
    // DL Reader General Settings
    //

    /**
     * Get the long poll time out for read last add confirmed requests, in milliseconds.
     * The default value is 1 second.
     *
     * @return long poll timeout in milliseconds
     * @see #getReadLACLongPollTimeout()
     */
    public int getReadLACLongPollTimeout() {
        return this.getInt(BKDL_READLACLONGPOLL_TIMEOUT, BKDL_READLACLONGPOLL_TIMEOUT_DEFAULT);
    }

    /**
     * Set the long poll time out for read last add confirmed requests, in milliseconds.
     *
     * @param readAheadLongPollTimeout long poll timeout in milliseconds
     * @return distributed log configuration
     * @see #getReadLACLongPollTimeout()
     */
    public DistributedLogConfiguration setReadLACLongPollTimeout(int readAheadLongPollTimeout) {
        setProperty(BKDL_READLACLONGPOLL_TIMEOUT, readAheadLongPollTimeout);
        return this;
    }

    //
    // Idle reader settings
    //

    /**
     * Get the time in milliseconds as the threshold for when an idle reader should dump warnings
     * <p>The default value is 2 minutes.
     *
     * @return reader idle warn threshold in millis.
     * @see #getReaderIdleErrorThresholdMillis()
     */
    public int getReaderIdleWarnThresholdMillis() {
        return getInt(BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS,
                BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS_DEFAULT);
    }

    /**
     * Set the time in milliseconds as the threshold for when an idle reader should dump warnings
     *
     * @param warnThreshold time after which we should dump the read ahead state
     * @return distributed log configuration
     * @see #getReaderIdleWarnThresholdMillis()
     */
    public DistributedLogConfiguration setReaderIdleWarnThresholdMillis(int warnThreshold) {
        setProperty(BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS, warnThreshold);
        return this;
    }

    /**
     * Get the time in milliseconds as the threshold for when an idle reader should throw errors
     * <p>The default value is <i>Integer.MAX_VALUE</i>.
     *
     * @return reader idle error threshold in millis
     * @see #getReaderIdleWarnThresholdMillis()
     */
    public int getReaderIdleErrorThresholdMillis() {
        return getInt(BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS,
                BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT);
    }

    /**
     * Set the time in milliseconds as the threshold for when an idle reader should throw errors
     *
     * @param warnThreshold time after which we should throw idle reader errors
     * @return distributed log configuration
     * @see #getReaderIdleErrorThresholdMillis()
     */
    public DistributedLogConfiguration setReaderIdleErrorThresholdMillis(int warnThreshold) {
        setProperty(BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS, warnThreshold);
        return this;
    }

    //
    // Reader Constraint Settings
    //

    /**
     * Get if we should ignore truncation status when reading the records
     *
     * @return if we should ignore truncation status
     */
    public boolean getIgnoreTruncationStatus() {
        return getBoolean(BKDL_READER_IGNORE_TRUNCATION_STATUS, BKDL_READER_IGNORE_TRUNCATION_STATUS_DEFAULT);
    }

    /**
     * Set if we should ignore truncation status when reading the records
     *
     * @param ignoreTruncationStatus
     *          if we should ignore truncation status
     */
    public DistributedLogConfiguration setIgnoreTruncationStatus(boolean ignoreTruncationStatus) {
        setProperty(BKDL_READER_IGNORE_TRUNCATION_STATUS, ignoreTruncationStatus);
        return this;
    }

    /**
     * Get if we should alert when reader is positioned on a truncated segment
     *
     * @return if we should alert when reader is positioned on a truncated segment
     */
    public boolean getAlertWhenPositioningOnTruncated() {
        return getBoolean(BKDL_READER_ALERT_POSITION_ON_TRUNCATED, BKDL_READER_ALERT_POSITION_ON_TRUNCATED_DEFAULT);
    }

    /**
     * Set if we should alert when reader is positioned on a truncated segment
     *
     * @param alertWhenPositioningOnTruncated
     *          if we should alert when reader is positioned on a truncated segment
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setAlertWhenPositioningOnTruncated(boolean alertWhenPositioningOnTruncated) {
        setProperty(BKDL_READER_ALERT_POSITION_ON_TRUNCATED, alertWhenPositioningOnTruncated);
        return this;
    }

    /**
     * Get whether position gap detection for reader enabled.
     * @return whether position gap detection for reader enabled.
     */
    public boolean getPositionGapDetectionEnabled() {
        return getBoolean(BKDL_READER_POSITION_GAP_DETECTION_ENABLED, BKDL_READER_POSITION_GAP_DETECTION_ENABLED_DEFAULT);
    }

    /**
     * Set if enable position gap detection for reader.
     *
     * @param enabled
     *          flag to enable/disable position gap detection on reader.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setPositionGapDetectionEnabled(boolean enabled) {
        setProperty(BKDL_READER_POSITION_GAP_DETECTION_ENABLED, enabled);
        return this;
    }

    //
    // ReadAhead Settings
    //

    /**
     * Set if we should enable read ahead.
     * By default is it enabled.
     *
     * @param enableReadAhead
     *          Enable read ahead
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setEnableReadAhead(boolean enableReadAhead) {
        setProperty(BKDL_ENABLE_READAHEAD, enableReadAhead);
        return this;
    }

    /**
     * Get if we should enable read ahead
     *
     * @return if read ahead is enabled
     */
    public boolean getEnableReadAhead() {
        return getBoolean(BKDL_ENABLE_READAHEAD, BKDL_ENABLE_READAHEAD_DEFAULT);
    }

    /**
     * Set if we should enable force read
     *
     * @param enableForceRead
     *          Enable force read
     */
    public DistributedLogConfiguration setEnableForceRead(boolean enableForceRead) {
        setProperty(BKDL_ENABLE_FORCEREAD, enableForceRead);
        return this;
    }

    /**
     * Get if we should enable force read
     *
     * @return if should use separate ZK Clients
     */
    public boolean getEnableForceRead() {
        return getBoolean(BKDL_ENABLE_FORCEREAD, BKDL_ENABLE_FORCEREAD_DEFAULT);
    }

    /**
     * Get the max records cached by readahead cache.
     * <p>The default value is 10. Increase this value to improve throughput,
     * but be careful about the memory.
     *
     * @return max records cached by readahead cache.
     */
    public int getReadAheadMaxRecords() {
        return this.getInt(BKDL_READAHEAD_MAX_RECORDS,
                getInt(BKDL_READAHEAD_MAX_RECORDS_OLD,
                        BKDL_READAHEAD_MAX_RECORDS_DEFAULT));
    }

    /**
     * Set the maximum records allowed to be cached by read ahead worker.
     *
     * @param readAheadMaxEntries max records to cache.
     * @return distributed log configuration
     * @see #getReadAheadMaxRecords()
     */
    public DistributedLogConfiguration setReadAheadMaxRecords(int readAheadMaxEntries) {
        setProperty(BKDL_READAHEAD_MAX_RECORDS, readAheadMaxEntries);
        return this;
    }

    /**
     * Get number of entries read as a batch by readahead worker.
     * <p>The default value is 2. Increase the value to increase the concurrency
     * of reading entries from bookkeeper.
     *
     * @return number of entries read as a batch.
     */
    public int getReadAheadBatchSize() {
        return this.getInt(BKDL_READAHEAD_BATCHSIZE,
                getInt(BKDL_READAHEAD_BATCHSIZE_OLD,
                        BKDL_READAHEAD_BATCHSIZE_DEFAULT));
    }

    /**
     * Set number of entries read as a batch by readahead worker.
     *
     * @param readAheadBatchSize
     *          Read ahead batch size.
     * @return distributed log configuration
     * @see #getReadAheadBatchSize()
     */
    public DistributedLogConfiguration setReadAheadBatchSize(int readAheadBatchSize) {
        setProperty(BKDL_READAHEAD_BATCHSIZE, readAheadBatchSize);
        return this;
    }

    /**
     * Get the wait time between successive attempts to poll for new log records, in milliseconds.
     * The default value is 200 ms.
     *
     * @return read ahead wait time
     */
    public int getReadAheadWaitTime() {
        return this.getInt(BKDL_READAHEAD_WAITTIME,
                getInt(BKDL_READAHEAD_WAITTIME_OLD, BKDL_READAHEAD_WAITTIME_DEFAULT));
    }

    /**
     * Set the wait time between successive attempts to poll for new log records, in milliseconds
     *
     * @param readAheadWaitTime read ahead wait time
     * @return distributed log configuration
     * @see #getReadAheadWaitTime()
     */
    public DistributedLogConfiguration setReadAheadWaitTime(int readAheadWaitTime) {
        setProperty(BKDL_READAHEAD_WAITTIME, readAheadWaitTime);
        return this;
    }

    /**
     * Get the wait time if it reaches end of stream and
     * <b>there isn't any inprogress logsegment in the stream</b>, in millis.
     * <p>The default value is 10 seconds.
     *
     * @see #setReadAheadWaitTimeOnEndOfStream(int)
     * @return the wait time if it reaches end of stream and there isn't
     * any inprogress logsegment in the stream, in millis.
     */
    public int getReadAheadWaitTimeOnEndOfStream() {
        return this.getInt(BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM,
                getInt(BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM_OLD,
                        BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM_DEFAULT));
    }

    /**
     * Set the wait time that would be used for readahead to backoff polling
     * logsegments from zookeeper when it reaches end of stream and there isn't
     * any inprogress logsegment in the stream. The unit is millis.
     *
     * @param waitTime
     *          wait time that readahead used to backoff when reaching end of stream.
     * @return distributedlog configuration
     * @see #getReadAheadWaitTimeOnEndOfStream()
     */
    public DistributedLogConfiguration setReadAheadWaitTimeOnEndOfStream(int waitTime) {
        setProperty(BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM, waitTime);
        return this;
    }

    /**
     * If readahead keeps receiving {@link org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException} on
     * reading last add confirmed in given period, it would stop polling last add confirmed and re-initialize the ledger
     * handle and retry. The threshold is specified in milliseconds.
     * <p>The default value is 10 seconds.
     *
     * @return error threshold in milliseconds, that readahead will reinitialize ledger handle after keeping receiving
     * no such ledger exceptions.
     */
    public int getReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis() {
        return this.getInt(BKDL_READAHEAD_NOSUCHLEDGER_EXCEPTION_ON_READLAC_ERROR_THRESHOLD_MILLIS,
                           BKDL_READAHEAD_NOSUCHLEDGER_EXCEPTION_ON_READLAC_ERROR_THRESHOLD_MILLIS_DEFAULT);
    }

    /**
     * Set the error threshold that readahead will reinitialize ledger handle after keeping receiving no such ledger exceptions.
     *
     * @see #getReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis()
     * @param thresholdMillis
     *          error threshold in milliseconds, that readahead will reinitialize ledger handle after keeping receiving
     *          no such ledger exceptions.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis(long thresholdMillis) {
        setProperty(BKDL_READAHEAD_NOSUCHLEDGER_EXCEPTION_ON_READLAC_ERROR_THRESHOLD_MILLIS, thresholdMillis);
        return this;
    }

    /**
     * When corruption is encountered in an entry, skip it and move on. Must disable gap detection for
     * this to work.
     *
     * @return should broken records be skipped
     */
    public boolean getReadAheadSkipBrokenEntries() {
        return getBoolean(BKDL_READAHEAD_SKIP_BROKEN_ENTRIES, BKDL_READAHEAD_SKIP_BROKEN_ENTRIES_DEFAULT);
    }

    /**
     * Set the percentage of operations to delay in read ahead.
     *
     * @param enabled
     *          should brokenn records be skipped
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setReadAheadSkipBrokenEntries(boolean enabled) {
        setProperty(BKDL_READAHEAD_SKIP_BROKEN_ENTRIES, enabled);
        return this;
    }

    //
    // DL Reader Scan Settings
    //

    /**
     * Number of entries to scan for first scan of reading last record.
     *
     * @return number of entries to scan for first scan of reading last record.
     */
    public int getFirstNumEntriesPerReadLastRecordScan() {
        return getInt(BKDL_FIRST_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN, BKDL_FIRST_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN_DEFAULT);
    }

    /**
     * Set number of entries to scan for first scan of reading last record.
     *
     * @param numEntries
     *          number of entries to scan
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setFirstNumEntriesPerReadLastRecordScan(int numEntries) {
        setProperty(BKDL_FIRST_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN, numEntries);
        return this;
    }

    /**
     * Max number of entries for each scan to read last record.
     *
     * @return max number of entries for each scan to read last record.
     */
    public int getMaxNumEntriesPerReadLastRecordScan() {
        return getInt(BKDL_MAX_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN, BKDL_MAX_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN_DEFAULT);
    }

    /**
     * Set max number of entries for each scan to read last record.
     *
     * @param numEntries
     *          number of entries to scan
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setMaxNumEntriesPerReadLastRecordScan(int numEntries) {
        setProperty(BKDL_MAX_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN, numEntries);
        return this;
    }

    //
    // DL Reader Log Existence Checking Settings
    //

    /**
     * Get the backoff start time to check log existence if the log doesn't exist.
     *
     * @return the backoff start time to check log existence if the log doesn't exist.
     */
    public long getCheckLogExistenceBackoffStartMillis() {
        return getLong(BKDL_CHECK_LOG_EXISTENCE_BACKOFF_START_MS, BKDL_CHECK_LOG_EXISTENCE_BACKOFF_START_MS_DEFAULT);
    }

    /**
     * Set the backoff start time to check log existence if the log doesn't exist.
     *
     * @param backoffMillis
     *          backoff time in millis
     * @return dl configuration
     */
    public DistributedLogConfiguration setCheckLogExistenceBackoffStartMillis(long backoffMillis) {
        setProperty(BKDL_CHECK_LOG_EXISTENCE_BACKOFF_START_MS, backoffMillis);
        return this;
    }

    /**
     * Get the backoff max time to check log existence if the log doesn't exist.
     *
     * @return the backoff max time to check log existence if the log doesn't exist.
     */
    public long getCheckLogExistenceBackoffMaxMillis() {
        return getLong(BKDL_CHECK_LOG_EXISTENCE_BACKOFF_MAX_MS, BKDL_CHECK_LOG_EXISTENCE_BACKOFF_MAX_MS_DEFAULT);
    }

    /**
     * Set the backoff max time to check log existence if the log doesn't exist.
     *
     * @param backoffMillis
     *          backoff time in millis
     * @return dl configuration
     */
    public DistributedLogConfiguration setCheckLogExistenceBackoffMaxMillis(long backoffMillis) {
        setProperty(BKDL_CHECK_LOG_EXISTENCE_BACKOFF_MAX_MS, backoffMillis);
        return this;
    }

    //
    // Tracing/Stats Settings
    //

    /**
     * Whether to trace read ahead delivery latency or not?
     *
     * @return flag to trace read ahead delivery latency.
     */
    public boolean getTraceReadAheadDeliveryLatency() {
        return getBoolean(BKDL_TRACE_READAHEAD_DELIVERY_LATENCY, BKDL_TRACE_READAHEAD_DELIVERY_LATENCY_DEFAULT);
    }

    /**
     * Set the flag to trace readahead delivery latency.
     *
     * @param enabled
     *          flag to trace readahead delivery latency.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setTraceReadAheadDeliveryLatency(boolean enabled) {
        setProperty(BKDL_TRACE_READAHEAD_DELIVERY_LATENCY, enabled);
        return this;
    }

    /**
     * Get the warn threshold (in millis) of metadata access latency.
     *
     * @return warn threshold of metadata access latency, in millis.
     */
    public long getMetadataLatencyWarnThresholdMillis() {
        return getLong(BKDL_METADATA_LATENCY_WARN_THRESHOLD_MS, BKDL_METADATA_LATENCY_WARN_THRESHOLD_MS_DEFAULT);
    }

    /**
     * Set the warn threshold of metadata access latency, in millis.
     *
     * @param warnThresholdMillis
     *          warn threshold of metadata access latency, in millis
     * @return dl configuration
     */
    public DistributedLogConfiguration setMetadataLatencyWarnThresholdMillis(long warnThresholdMillis) {
        setProperty(BKDL_METADATA_LATENCY_WARN_THRESHOLD_MS, warnThresholdMillis);
        return this;
    }

    /**
     * Get the warn threshold (in millis) of data access latency.
     *
     * @return warn threshold of data access latency, in millis.
     */
    public long getDataLatencyWarnThresholdMillis() {
        return getLong(BKDL_DATA_LATENCY_WARN_THRESHOLD_MS, BKDL_DATA_LATENCY_WARN_THRESHOLD_MS_DEFAULT);
    }

    /**
     * Set the warn threshold of data access latency, in millis.
     *
     * @param warnThresholdMillis
     *          warn threshold of data access latency, in millis
     * @return dl configuration
     */
    public DistributedLogConfiguration setDataLatencyWarnThresholdMillis(long warnThresholdMillis) {
        setProperty(BKDL_DATA_LATENCY_WARN_THRESHOLD_MS, warnThresholdMillis);
        return this;
    }

    /**
     * Whether to trace read ahead changes? If enabled, it will log readahead metadata changes with timestamp.
     * It is helpful when you are troubleshooting latency related issues.
     *
     * @return flag to trace read ahead delivery latency.
     */
    public boolean getTraceReadAheadMetadataChanges() {
        return getBoolean(BKDL_TRACE_READAHEAD_METADATA_CHANGES, BKDL_TRACE_READAHEAD_MEATDATA_CHANGES_DEFAULT);
    }

    /**
     * Set the flag to trace readahead metadata changes.
     *
     * @see #getTraceReadAheadMetadataChanges()
     *
     * @param enabled
     *          flag to trace readahead metadata changes.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setTraceReadAheadMetadataChanges(boolean enabled) {
        setProperty(BKDL_TRACE_READAHEAD_METADATA_CHANGES, enabled);
        return this;
    }

    /**
     * Whether to trace long running tasks and record task execution stats in thread pools.
     *
     * @return flag to enable task execution stats
     */
    public boolean getEnableTaskExecutionStats() {
        return getBoolean(BKDL_ENABLE_TASK_EXECUTION_STATS, BKDL_ENABLE_TASK_EXECUTION_STATS_DEFAULT);
    }

    /**
     * Set to trace long running tasks and record task execution stats in thread pools.
     *
     * @see #getEnableTaskExecutionStats()
     *
     * @param enabled
     *          flag to enable task execution stats.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setEnableTaskExecutionStats(boolean enabled) {
        setProperty(BKDL_ENABLE_TASK_EXECUTION_STATS, enabled);
        return this;
    }

    /**
     * Report long running task after execution takes longer than the given interval.
     *
     * @return warn time for long running tasks
     */
    public long getTaskExecutionWarnTimeMicros() {
        return getLong(BKDL_TASK_EXECUTION_WARN_TIME_MICROS, BKDL_TASK_EXECUTION_WARN_TIME_MICROS_DEFAULT);
    }

    /**
     * Set warn time for reporting long running tasks.
     *
     * @see #getTaskExecutionWarnTimeMicros()
     *
     * @param warnTimeMicros
     *          warn time for long running tasks.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setTaskExecutionWarnTimeMicros(long warnTimeMicros) {
        setProperty(BKDL_TASK_EXECUTION_WARN_TIME_MICROS, warnTimeMicros);
        return this;
    }

    /**
     * Whether to enable per stream stat or not.
     *
     * @deprecated please use {@link DistributedLogNamespaceBuilder#perLogStatsLogger(StatsLogger)}
     * @return flag to enable per stream stat.
     */
    public boolean getEnablePerStreamStat() {
        return getBoolean(BKDL_ENABLE_PERSTREAM_STAT, BKDL_ENABLE_PERSTREAM_STAT_DEFAULT);
    }

    /**
     * Set the flag to enable per stream stat or not.
     *
     * @deprecated please use {@link DistributedLogNamespaceBuilder#perLogStatsLogger(StatsLogger)}
     * @param enabled
     *          flag to enable/disable per stream stat.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setEnablePerStreamStat(boolean enabled) {
        setProperty(BKDL_ENABLE_PERSTREAM_STAT, enabled);
        return this;
    }

    //
    // Settings for Feature Providers
    //

    /**
     * Get feature provider class.
     *
     * @return feature provider class.
     * @throws ConfigurationException
     */
    public Class<? extends FeatureProvider> getFeatureProviderClass()
            throws ConfigurationException {
        return ReflectionUtils.getClass(this, BKDL_FEATURE_PROVIDER_CLASS, DefaultFeatureProvider.class,
                FeatureProvider.class, FeatureProvider.class.getClassLoader());
    }

    /**
     * Set feature provider class.
     *
     * @param providerClass
     *          feature provider class.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setFeatureProviderClass(Class<? extends FeatureProvider> providerClass) {
        setProperty(BKDL_FEATURE_PROVIDER_CLASS, providerClass.getName());
        return this;
    }

    /**
     * Get the base config path for file feature provider.
     *
     * @return base config path for file feature provider.
     */
    public String getFileFeatureProviderBaseConfigPath() {
        return getString(BKDL_FILE_FEATURE_PROVIDER_BASE_CONFIG_PATH,
                BKDL_FILE_FEATURE_PROVIDER_BASE_CONFIG_PATH_DEFAULT);
    }

    /**
     * Set the base config path for file feature provider.
     *
     * @param conf
     *          distributedlog configuration
     * @param configPath
     *          base config path for file feature provider.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setFileFeatureProviderBaseConfigPath(String configPath) {
        setProperty(BKDL_FILE_FEATURE_PROVIDER_BASE_CONFIG_PATH, configPath);
        return this;
    }

    /**
     * Get the overlay config path for file feature provider.
     *
     * @return overlay config path for file feature provider.
     */
    public String getFileFeatureProviderOverlayConfigPath() {
        return getString(BKDL_FILE_FEATURE_PROVIDER_OVERLAY_CONFIG_PATH,
                BKDL_FILE_FEATURE_PROVIDER_OVERLAY_CONFIG_PATH_DEFAULT);
    }

    /**
     * Set the overlay config path for file feature provider.
     *
     * @param conf distributedlog configuration
     * @param configPath
     *          overlay config path for file feature provider.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setFileFeatureProviderOverlayConfigPath(String configPath) {
        setProperty(BKDL_FILE_FEATURE_PROVIDER_OVERLAY_CONFIG_PATH,
                configPath);
        return this;
    }

    //
    // Settings for Namespaces
    //

    /**
     * Is federated namespace implementation enabled.
     *
     * @return true if federated namespace is enabled. otherwise, false.
     */
    public boolean isFederatedNamespaceEnabled() {
        return getBoolean(BKDL_FEDERATED_NAMESPACE_ENABLED, BKDL_FEDERATED_NAMESPACE_ENABLED_DEFAULT);
    }

    /**
     * Use federated namespace implementation if this flag is enabled.
     *
     * @param enabled flag to enable federated namespace implementation
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setFederatedNamespaceEnabled(boolean enabled) {
        setProperty(BKDL_FEDERATED_NAMESPACE_ENABLED, enabled);
        return this;
    }

    /**
     * Get the max logs per sub namespace for federated namespace.
     *
     * @return max logs per sub namespace
     */
    public int getFederatedMaxLogsPerSubnamespace() {
        return getInt(BKDL_FEDERATED_MAX_LOGS_PER_SUBNAMESPACE, BKDL_FEDERATED_MAX_LOGS_PER_SUBNAMESPACE_DEFAULT);
    }

    /**
     * Set the max logs per sub namespace for federated namespace.
     *
     * @param maxLogs
     *          max logs per sub namespace
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setFederatedMaxLogsPerSubnamespace(int maxLogs) {
        setProperty(BKDL_FEDERATED_MAX_LOGS_PER_SUBNAMESPACE, maxLogs);
        return this;
    }

    /**
     * Whether check the existence of a log if querying local cache of a federated namespace missed.
     * Enabling it will issue zookeeper queries to check all sub namespaces under a federated namespace.
     *
     * NOTE: by default it is on for all admin related tools. for write proxies, consider turning off for
     * performance.
     *
     * @return true if it needs to check existence of a log when querying local cache misses. otherwise false.
     */
    public boolean getFederatedCheckExistenceWhenCacheMiss() {
        return getBoolean(BKDL_FEDERATED_CHECK_EXISTENCE_WHEN_CACHE_MISS,
                BKDL_FEDERATED_CHECK_EXISTENCE_WHEN_CACHE_MISS_DEFAULT);
    }

    /**
     * Enable check existence of a log if quering local cache of a federated namespace missed.
     *
     * @param enabled
     *          flag to enable/disable this feature.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setFederatedCheckExistenceWhenCacheMiss(boolean enabled) {
        setProperty(BKDL_FEDERATED_CHECK_EXISTENCE_WHEN_CACHE_MISS, enabled);
        return this;
    }

    //
    // Settings for Configurations
    //

    /**
     * Get dynamic configuration reload interval in seconds.
     *
     * @return dynamic configuration reload interval
     */
    public int getDynamicConfigReloadIntervalSec() {
        return getInt(BKDL_DYNAMIC_CONFIG_RELOAD_INTERVAL_SEC, BKDL_DYNAMIC_CONFIG_RELOAD_INTERVAL_SEC_DEFAULT);
    }

    /**
     * Get dynamic configuration reload interval in seconds.
     *
     * @param intervalSec dynamic configuration reload interval in seconds
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setDynamicConfigReloadIntervalSec(int intervalSec) {
        setProperty(BKDL_DYNAMIC_CONFIG_RELOAD_INTERVAL_SEC, intervalSec);
        return this;
    }

    /**
     * Get config router class which determines how stream name is mapped to configuration.
     *
     * @return config router class.
     */
    public String getStreamConfigRouterClass() {
        return getString(BKDL_STREAM_CONFIG_ROUTER_CLASS, BKDL_STREAM_CONFIG_ROUTER_CLASS_DEFAULT);
    }

    /**
     * Set config router class.
     *
     * @param routerClass
     *          config router class.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setStreamConfigRouterClass(String routerClass) {
        setProperty(BKDL_STREAM_CONFIG_ROUTER_CLASS, routerClass);
        return this;
    }

    //
    // Settings for RateLimit
    //

    /**
     * A lower threshold bytes per second limit on writes to the distributedlog proxy.
     *
     * @return Bytes per second write limit
     */
    public int getBpsSoftWriteLimit() {
        return getInt(BKDL_BPS_SOFT_WRITE_LIMIT, BKDL_BPS_SOFT_WRITE_LIMIT_DEFAULT);
    }

    /**
     * An upper threshold bytes per second limit on writes to the distributedlog proxy.
     *
     * @return Bytes per second write limit
     */
    public int getBpsHardWriteLimit() {
        return getInt(BKDL_BPS_HARD_WRITE_LIMIT, BKDL_BPS_HARD_WRITE_LIMIT_DEFAULT);
    }

    /**
     * A lower threshold requests per second limit on writes to the distributedlog proxy.
     *
     * @return Requests per second write limit
     */
    public int getRpsSoftWriteLimit() {
        return getInt(BKDL_RPS_SOFT_WRITE_LIMIT, BKDL_RPS_SOFT_WRITE_LIMIT_DEFAULT);
    }

    /**
     * An upper threshold requests per second limit on writes to the distributedlog proxy.
     *
     * @return Requests per second write limit
     */
    public int getRpsHardWriteLimit() {
        return getInt(BKDL_RPS_HARD_WRITE_LIMIT, BKDL_RPS_HARD_WRITE_LIMIT_DEFAULT);
    }

    //
    // Settings for partitioning
    //

    /**
     * Get the maximum number of partitions of each stream allowed to be acquired per proxy.
     * <p>This setting is able to configure per stream. This is the default setting if it is
     * not configured per stream. Default value is -1, which means no limit on the number of
     * partitions could be acquired each stream.
     *
     * @return maximum number of partitions of each stream allowed to be acquired per proxy.
     */
    public int getMaxAcquiredPartitionsPerProxy() {
        return getInt(BKDL_MAX_ACQUIRED_PARTITIONS_PER_PROXY, BKDL_MAX_ACQUIRED_PARTITIONS_PER_PROXY_DEFAULT);
    }

    /**
     * Set the maximum number of partitions of each stream allowed to be acquired per proxy.
     *
     * @param numPartitions
     *          number of partitions of each stream allowed to be acquired
     * @return distributedlog configuration
     * @see #getMaxAcquiredPartitionsPerProxy()
     */
    public DistributedLogConfiguration setMaxAcquiredPartitionsPerProxy(int numPartitions) {
        setProperty(BKDL_MAX_ACQUIRED_PARTITIONS_PER_PROXY, numPartitions);
        return this;
    }

    /**
     * Get the maximum number of partitions of each stream allowed to cache per proxy.
     * <p>This setting is able to configure per stream. This is the default setting if it is
     * not configured per stream. Default value is -1, which means no limit on the number of
     * partitions could be acquired each stream.
     *
     * @return maximum number of partitions of each stream allowed to be acquired per proxy.
     */
    public int getMaxCachedPartitionsPerProxy() {
        return getInt(BKDL_MAX_CACHED_PARTITIONS_PER_PROXY, BKDL_MAX_CACHED_PARTITIONS_PER_PROXY_DEFAULT);
    }

    /**
     * Set the maximum number of partitions of each stream allowed to cache per proxy.
     *
     * @param numPartitions
     *          number of partitions of each stream allowed to cache
     * @return distributedlog configuration
     * @see #getMaxAcquiredPartitionsPerProxy()
     */
    public DistributedLogConfiguration setMaxCachedPartitionsPerProxy(int numPartitions) {
        setProperty(BKDL_MAX_CACHED_PARTITIONS_PER_PROXY, numPartitions);
        return this;
    }

    // Error Injection Settings

    /**
     * Should we enable write delay injection? If false we won't check other write delay settings.
     *
     * @return true if write delay injection is enabled.
     */
    public boolean getEIInjectWriteDelay() {
        return getBoolean(BKDL_EI_INJECT_WRITE_DELAY, BKDL_EI_INJECT_WRITE_DELAY_DEFAULT);
    }

    /**
     * Get percent of write requests which should be delayed by BKDL_EI_INJECTED_WRITE_DELAY_MS.
     *
     * @return percent of writes to delay.
     */
    public double getEIInjectedWriteDelayPercent() {
        return getDouble(BKDL_EI_INJECTED_WRITE_DELAY_PERCENT, BKDL_EI_INJECTED_WRITE_DELAY_PERCENT_DEFAULT);
    }

    /**
     * Set percent of write requests which should be delayed by BKDL_EI_INJECTED_WRITE_DELAY_MS. 0 disables
     * write delay.
     *
     * @param percent
     *          percent of writes to delay.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setEIInjectedWriteDelayPercent(double percent) {
        setProperty(BKDL_EI_INJECTED_WRITE_DELAY_PERCENT, percent);
        return this;
    }

    /**
     * Get amount of time to delay writes for in writer failure injection.
     *
     * @return millis to delay writes for.
     */
    public int getEIInjectedWriteDelayMs() {
        return getInt(BKDL_EI_INJECTED_WRITE_DELAY_MS, BKDL_EI_INJECTED_WRITE_DELAY_MS_DEFAULT);
    }

    /**
     * Set amount of time to delay writes for in writer failure injection. 0 disables write delay.
     *
     * @param delayMs
     *          ms to delay writes for.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setEIInjectedWriteDelayMs(int delayMs) {
        setProperty(BKDL_EI_INJECTED_WRITE_DELAY_MS, delayMs);
        return this;
    }

    /**
     * Get the flag whether to inject stalls in read ahead.
     *
     * @return true if to inject stalls in read ahead, otherwise false.
     */
    public boolean getEIInjectReadAheadStall() {
        return getBoolean(BKDL_EI_INJECT_READAHEAD_STALL, BKDL_EI_INJECT_READAHEAD_STALL_DEFAULT);
    }

    /**
     * Set the flag whether to inject stalls in read ahead.
     *
     * @param enabled
     *          flag to inject stalls in read ahead.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setEIInjectReadAheadStall(boolean enabled) {
        setProperty(BKDL_EI_INJECT_READAHEAD_STALL, enabled);
        return this;
    }

    /**
     * Get the flag whether to inject broken entries in readahead.
     *
     * @return true if to inject corruption in read ahead, otherwise false.
     */
    public boolean getEIInjectReadAheadBrokenEntries() {
        return getBoolean(BKDL_EI_INJECT_READAHEAD_BROKEN_ENTRIES, BKDL_EI_INJECT_READAHEAD_BROKEN_ENTRIES_DEFAULT);
    }

    /**
     * Set the flag whether to inject broken entries in read ahead.
     *
     * @param enabled
     *          flag to inject corruption in read ahead.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setEIInjectReadAheadBrokenEntries(boolean enabled) {
        setProperty(BKDL_EI_INJECT_READAHEAD_BROKEN_ENTRIES, enabled);
        return this;
    }

    /**
     * Get the flag whether to inject delay in read ahead.
     *
     * @return true if to inject delays in read ahead, otherwise false.
     */
    public boolean getEIInjectReadAheadDelay() {
        return getBoolean(BKDL_EI_INJECT_READAHEAD_DELAY, BKDL_EI_INJECT_READAHEAD_DELAY_DEFAULT);
    }

    /**
     * Set the flag whether to inject delays in read ahead.
     *
     * @param enabled
     *          flag to inject delays in read ahead.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setEIInjectReadAheadDelay(boolean enabled) {
        setProperty(BKDL_EI_INJECT_READAHEAD_DELAY, enabled);
        return this;
    }

    /**
     * Get the max injected delay in read ahead, in millis.
     *
     * @return max injected delay in read ahead, in millis.
     */
    public int getEIInjectMaxReadAheadDelayMs() {
        return getInt(BKDL_EI_INJECT_MAX_READAHEAD_DELAY_MS, BKDL_EI_INJECT_MAX_READAHEAD_DELAY_MS_DEFAULT);
    }

    /**
     * Set the max injected delay in read ahead, in millis.
     *
     * @param delayMs
     *          max injected delay in read ahead, in millis.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setEIInjectMaxReadAheadDelayMs(int delayMs) {
        setProperty(BKDL_EI_INJECT_MAX_READAHEAD_DELAY_MS, delayMs);
        return this;
    }

    /**
     * Get the percentage of operations to delay in read ahead.
     *
     * @return the percentage of operations to delay in read ahead.
     */
    public int getEIInjectReadAheadDelayPercent() {
        return getInt(BKDL_EI_INJECT_READAHEAD_DELAY_PERCENT, BKDL_EI_INJECT_READAHEAD_DELAY_PERCENT_DEFAULT);
    }

    /**
     * Set the percentage of operations to delay in read ahead.
     *
     * @param percent
     *          the percentage of operations to delay in read ahead.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setEIInjectReadAheadDelayPercent(int percent) {
        setProperty(BKDL_EI_INJECT_READAHEAD_DELAY_PERCENT, percent);
        return this;
    }


}
