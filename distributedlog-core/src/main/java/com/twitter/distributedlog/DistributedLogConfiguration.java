package com.twitter.distributedlog;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.feature.DeciderFeatureProvider;
import org.apache.bookkeeper.feature.FeatureProvider;
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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class DistributedLogConfiguration extends CompositeConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogConfiguration.class);

    private static ClassLoader defaultLoader;

    static {
        defaultLoader = Thread.currentThread().getContextClassLoader();
        if (null == defaultLoader) {
            defaultLoader = DistributedLogConfiguration.class.getClassLoader();
        }
    }

    public static final String BKDL_ZK_ACL_ID = "zkAclId";
    public static final String BKDL_ZK_ACL_ID_DEFAULT = null;

    public static final String BKDL_LEDGER_METADATA_LAYOUT_VERSION = "ledger-metadata-layout";
    public static final int BKDL_LEDGER_METADATA_LAYOUT_VERSION_DEFAULT =
            LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V5_SEQUENCE_ID.value;

    public static final String BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK = "ledgerMetadataSkipMinVersionCheck";
    public static final boolean BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK_DEFAULT = false;

    public static final String BKDL_FIRST_LEDGER_SEQUENCE_NUMBER = "first-ledger-sequence-number";
    public static final long BKDL_FIRST_LEDGER_SEQUENCE_NUMBER_DEFAULT = DistributedLogConstants.FIRST_LEDGER_SEQNO;

    // Name for the default (non-partitioned) stream
    public static final String BKDL_UNPARTITIONED_STREAM_NAME = "unpartitionedStreamName";
    public static final String BKDL_UNPARTITIONED_STREAM_NAME_DEFAULT = "<default>";

    // Controls when log records accumulated in the writer will be
    // transmitted to bookkeeper
    public static final String BKDL_OUTPUT_BUFFER_SIZE = "output-buffer-size";
    public static final int BKDL_OUTPUT_BUFFER_SIZE_DEFAULT = 1024;

    public static final String BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS = "periodicFlushFrequencyMilliSeconds";
    public static final int BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT = 0;

    public static final String BKDL_ENABLE_IMMEDIATE_FLUSH = "enableImmediateFlush";
    public static final boolean BKDL_ENABLE_IMMEDIATE_FLUSH_DEFAULT = false;

    public static final String BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS = "minimumDelayBetweenImmediateFlushMilliSeconds";
    public static final int BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS_DEFAULT = 0;

    // Controls the retention period after which old ledgers are deleted
    public static final String BKDL_RETENTION_PERIOD_IN_HOURS = "retention-size";
    public static final int BKDL_RETENTION_PERIOD_IN_HOURS_DEFAULT = 72;

    public static final String BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION = "explicitTruncationByApp";
    public static final boolean BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION_DEFAULT = false;

    // The time after which the a given log stream switches to a new ledger
    public static final String BKDL_ROLLING_INTERVAL_IN_MINUTES = "rolling-interval";
    public static final int BKDL_ROLLING_INTERVAL_IN_MINUTES_DEFAULT = 120;

    // Max LogSegment Bytes
    public static final String BKDL_MAX_LOGSEGMENT_BYTES = "maxLogSegmentBytes";
    public static final int BKDL_MAX_LOGSEGMENT_BYTES_DEFAULT = 256 * 1024 * 1024; // default 256MB

    public static final String BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT = "row-aware-ensemble-placement";
    public static final boolean BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT_DEFAULT = false;

    // Bookkeeper ensemble size
    public static final String BKDL_BOOKKEEPER_ENSEMBLE_SIZE = "ensemble-size";
    public static final int BKDL_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT = 3;

    // Bookkeeper write quorum size
    public static final String BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE = "write-quorum-size";
    public static final int BKDL_BOOKKEEPER_WRITE_QUORUM_SIZE_DEFAULT = 2;

    // Bookkeeper ack quorum size
    public static final String BKDL_BOOKKEEPER_ACK_QUORUM_SIZE = "ack-quorum-size";
    public static final int BKDL_BOOKKEEPER_ACK_QUORUM_SIZE_DEFAULT = 2;

    // Bookkeeper digest
    public static final String BKDL_BOOKKEEPER_DIGEST_PW = "digestPw";
    public static final String BKDL_BOOKKEEPER_DIGEST_PW_DEFAULT = "";

    // Executor Parameters
    public static final String BKDL_NUM_WORKER_THREADS = "numWorkerThreads";
    public static final String BKDL_NUM_READAHEAD_WORKER_THREADS = "numReadAheadWorkerThreads";
    public static final String BKDL_NUM_LOCKSTATE_THREADS = "numLockStateThreads";

    // Reader parameters
    public static final String BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS = "readerIdleWarnThresholdMillis";
    public static final int BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS_DEFAULT = 120000;

    public static final String BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS = "readerIdleErrorThresholdMillis";
    public static final int BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT = Integer.MAX_VALUE;

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

    public static final String BKDL_READAHEAD_MAX_ENTRIES = "ReadAheadMaxEntries";
    public static final int BKDL_READAHEAD_MAX_ENTRIES_DEFAULT = 10;

    public static final String BKDL_READAHEAD_BATCHSIZE = "ReadAheadBatchSize";
    public static final int BKDL_READAHEAD_BATCHSIZE_DEFAULT = 2;

    public static final String BKDL_READAHEAD_WAITTIME = "ReadAheadWaitTime";
    public static final int BKDL_READAHEAD_WAITTIME_DEFAULT = 200;

    public static final String BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM = "ReadAheadWaitTimeOnEndOfStream";
    public static final int BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM_DEFAULT = 10000;

    public static final String BKDL_READAHEAD_NOSUCHLEDGER_EXCEPTION_ON_READLAC_ERROR_THRESHOLD_MILLIS =
            "readAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis";
    public static final int BKDL_READAHEAD_NOSUCHLEDGER_EXCEPTION_ON_READLAC_ERROR_THRESHOLD_MILLIS_DEFAULT = 10000;

    public static final String BKDL_READLACLONGPOLL_TIMEOUT = "readLACLongPollTimeout";
    public static final int BKDL_READLACLONGPOLL_TIMEOUT_DEFAULT = 1000;

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

    public static final String BKDL_ENABLE_RECORD_COUNTS = "enableRecordCounts";
    public static final boolean BKDL_ENABLE_RECORD_COUNTS_DEFAULT = true;

    // Various timeouts - names are self explanatory
    public static final String BKDL_LOG_FLUSH_TIMEOUT = "logFlushTimeoutSeconds";
    public static final int BKDL_LOG_FLUSH_TIMEOUT_DEFAULT = 30;

    public static final String BKDL_LOCK_TIMEOUT = "lockTimeoutSeconds";
    public static final long BKDL_LOCK_TIMEOUT_DEFAULT = 30;

    // Advanced/internal lock timeouts
    public static final String BKDL_LOCK_REACQUIRE_TIMEOUT = "lockReacquireTimeoutSeconds";
    public static final long BKDL_LOCK_REACQUIRE_TIMEOUT_DEFAULT = DistributedLogConstants.LOCK_REACQUIRE_TIMEOUT_DEFAULT;

    public static final String BKDL_LOCK_OP_TIMEOUT = "lockOpTimeoutSeconds";
    public static final long BKDL_LOCK_OP_TIMEOUT_DEFAULT = DistributedLogConstants.LOCK_OP_TIMEOUT_DEFAULT;

    // Bkc config
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

    public static final String BKDL_BKCLIENT_READ_TIMEOUT = "bkcReadTimeoutSeconds";
    public static final int BKDL_BKCLIENT_READ_TIMEOUT_DEFAULT = 10;

    public static final String BKDL_BKCLIENT_WRITE_TIMEOUT = "bkcWriteTimeoutSeconds";
    public static final int BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT = 10;

    public static final String BKDL_BKCLIENT_NUM_WORKER_THREADS = "bkcNumWorkerThreads";
    public static final int BKDL_BKCLEINT_NUM_WORKER_THREADS_DEFAULT = 1;

    public static final String BKDL_BKCLIENT_NUM_IO_THREADS = "bkcNumIOThreads";
    public static final int BKDL_BKCLIENT_NUM_IO_THREADS_DEFAULT = Runtime.getRuntime().availableProcessors() * 2;

    public static final String BKDL_MAXID_SANITYCHECK = "maxIdSanityCheck";
    public static final boolean BKDL_MAXID_SANITYCHECK_DEFAULT = true;

    public static final String BKDL_ENABLE_LEDGER_ALLOCATOR_POOL = "enableLedgerAllocatorPool";
    public static final boolean BKDL_ENABLE_LEDGER_ALLOCATOR_POOL_DEFAULT = false;

    public static final String BKDL_LEDGER_ALLOCATOR_POOL_PATH = "ledgerAllocatorPoolPath";
    public static final String BKDL_LEDGER_ALLOCATOR_POOL_PATH_DEFAULT = DistributedLogConstants.ALLOCATION_POOL_NODE;

    public static final String BKDL_LEDGER_ALLOCATOR_POOL_NAME = "ledgerAllocatorPoolName";
    public static final String BKDL_LEDGER_ALLOCATOR_POOL_NAME_DEFAULT = null;

    public static final String BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE = "ledgerAllocatorPoolCoreSize";
    public static final int BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE_DEFAULT = 20;

    public static final String BKDL_CREATE_STREAM_IF_NOT_EXISTS = "createStreamIfNotExists";
    public static final boolean BKDL_CREATE_STREAM_IF_NOT_EXISTS_DEFAULT = true;

    public static final String BKDL_LOGSEGMENT_ROLLING_CONCURRENCY = "logSegmentRollingConcurrency";
    public static final int BKDL_LOGSEGMENT_ROLLING_CONCURRENCY_DEFAULT = 1;

    public static final String BKDL_ENCODE_REGION_ID_IN_VERSION = "encodeRegionIDInVersion";
    public static final boolean BKDL_ENCODE_REGION_ID_IN_VERSION_DEFAULT = false;

    public static final String BKDL_LOGSEGMENT_NAME_VERSION = "logSegmentNameVersion";
    public static final int BKDL_LOGSEGMENT_NAME_VERSION_DEFAULT = DistributedLogConstants.LOGSEGMENT_NAME_VERSION;

    public static final String BKDL_READLAC_OPTION = "readLACLongPoll";
    public static final int BKDL_READLAC_OPTION_DEFAULT = 3; //BKLogPartitionReadHandler.ReadLACOption.READENTRYPIGGYBACK_SEQUENTIAL.value

    public static final String BKDL_BK_DNS_RESOLVER_OVERRIDES = "dnsResolverOverrides";
    public static final String BKDL_BK_DNS_RESOLVER_OVERRIDES_DEFAULT = "";

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

    public static final String BKDL_ZKCLIENT_NUM_RETRY_THREADS = "zkcNumRetryThreads";
    public static final int BKDL_ZKCLIENT_NUM_RETRY_THREADS_DEFAULT = 1;

    public static final String BKDL_TIMEOUT_TIMER_TICK_DURATION_MS = "timerTickDuration";
    public static final long BKDL_TIMEOUT_TIMER_TICK_DURATION_MS_DEFAULT = 100;

    public static final String BKDL_TIMEOUT_TIMER_NUM_TICKS = "timerNumTicks";
    public static final int BKDL_TIMEOUT_TIMER_NUM_TICKS_DEFAULT = 1024;

    public static final String BKDL_FIRST_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN = "firstNumEntriesEachPerLastRecordScan";
    public static final int BKDL_FIRST_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN_DEFAULT = 2;
    public static final String BKDL_MAX_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN = "maxNumEntriesPerReadLastRecordScan";
    public static final int BKDL_MAX_NUM_ENTRIES_PER_READ_LAST_RECORD_SCAN_DEFAULT = 16;

    public static final String BKDL_CHECK_LOG_EXISTENCE_BACKOFF_START_MS = "checkLogExistenceBackoffStartMillis";
    public static final int BKDL_CHECK_LOG_EXISTENCE_BACKOFF_START_MS_DEFAULT = 200;

    public static final String BKDL_CHECK_LOG_EXISTENCE_BACKOFF_MAX_MS = "checkLogExistenceBackoffMaxMillis";
    public static final int BKDL_CHECK_LOG_EXISTENCE_BACKOFF_MAX_MS_DEFAULT = 1000;

    public static final String BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT = "perWriterOutstandingWriteLimit";
    public static final int BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT_DEFAULT = -1;

    public static final String BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT = "globalOutstandingWriteLimit";
    public static final int BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT_DEFAULT = -1;

    public static final String BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE = "outstandingWriteLimitDarkmode";
    public static final boolean BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE_DEFAULT = true;

    public static final String BKDL_FAILFAST_ON_STREAM_NOT_READY = "failFastOnStreamNotReady";
    public static final boolean BKDL_FAILFAST_ON_STREAM_NOT_READY_DEFAULT = false;

    public static final String BKDL_SERVICE_TIMEOUT_MS = "serviceTimeoutMs";
    public static final long BKDL_SERVICE_TIMEOUT_MS_DEFAULT = 0;

    public static final String BKDL_STREAM_PROBATION_TIMEOUT_MS = "streamProbationTimeoutMs";
    public static final long BKDL_STREAM_PROBATION_TIMEOUT_MS_DEFAULT = 60*1000*5;

    public static final String BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS = "schedulerShutdownTimeoutMs";
    public static final int BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS_DEFAULT = 5000;

    public static final String BKDL_USE_DAEMON_THREAD = "useDaemonThread";
    public static final boolean BKDL_USE_DAEMON_THREAD_DEFAULT = false;

    // Settings for Error Injection
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

    /**
     *  CompressionCodec.Type     String to use (See CompressionUtils)
     *  ---------------------     ------------------------------------
     *          NONE               none
     *          LZ4                lz4
     *          UNKNOWN            any other instance of String.class
     */
    public static final String BKDL_COMPRESSION_TYPE = "compressionType";
    public static final String BKDL_COMPRESSION_TYPE_DEFAULT = "none";

    // Settings for Deciders

    public static final String BKDL_FEATURE_PROVIDER_CLASS = "featureProviderClass";
    public static final String BKDL_DECIDER_BASE_CONFIG_PATH = "deciderBaseConfigPath";
    public static final String BKDL_DECIDER_BASE_CONFIG_PATH_DEFAULT = "decider.yml";
    public static final String BKDL_DECIDER_OVERLAY_CONFIG_PATH = "deciderOverlayConfigPath";
    public static final String BKDL_DECIDER_OVERLAY_CONFIG_PATH_DEFAULT = null;
    public static final String BKDL_DECIDER_ENVIRONMENT = "deciderEnvironment";
    public static final String BKDL_DECIDER_ENVIRONMENT_DEFAULT = null;

    // Settings for Namespaces

    public static final String BKDL_FEDERATED_MAX_LOGS_PER_SUBNAMESPACE = "federatedMaxLogsPerSubnamespace";
    public static final int BKDL_FEDERATED_MAX_LOGS_PER_SUBNAMESPACE_DEFAULT = 15000;
    public static final String BKDL_FEDERATED_CHECK_EXISTENCE_WHEN_CACHE_MISS = "federatedCheckExistenceWhenCacheMiss";
    public static final boolean BKDL_FEDERATED_CHECK_EXISTENCE_WHEN_CACHE_MISS_DEFAULT = true;

    // Settings for Configurations

    public static final String BKDL_DYNAMIC_CONFIG_RELOAD_INTERVAL_SEC = "dynamicConfigReloadIntervalSec";
    public static final int BKDL_DYNAMIC_CONFIG_RELOAD_INTERVAL_SEC_DEFAULT = 60;
    public static final String BKDL_STREAM_CONFIG_ROUTER_CLASS = "streamConfigRouterClass";
    public static final String BKDL_STREAM_CONFIG_ROUTER_CLASS_DEFAULT = "com.twitter.distributedlog.service.config.IdentityConfigRouter";

    // Settings for RateLimit

    public static final String BKDL_BPS_SOFT_WRITE_LIMIT = "bpsSoftWriteLimit";
    public static final int BKDL_BPS_SOFT_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_BPS_HARD_WRITE_LIMIT = "bpsHardWriteLimit";
    public static final int BKDL_BPS_HARD_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_RPS_SOFT_WRITE_LIMIT = "rpsSoftWriteLimit";
    public static final int BKDL_RPS_SOFT_WRITE_LIMIT_DEFAULT = -1;
    public static final String BKDL_RPS_HARD_WRITE_LIMIT = "rpsHardWriteLimit";
    public static final int BKDL_RPS_HARD_WRITE_LIMIT_DEFAULT = -1;

    // Whitelisted stream-level configuration settings.
    private static final Set<String> streamSettings = Sets.newHashSet(
        BKDL_READER_POSITION_GAP_DETECTION_ENABLED,
        BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS,
        BKDL_READER_IDLE_WARN_THRESHOLD_MILLIS,
        BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS,
        BKDL_ENABLE_IMMEDIATE_FLUSH
    );

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
     * @return stream configuration
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
     */
    public void setZkAclId(String zkAclId) {
        setProperty(BKDL_ZK_ACL_ID, zkAclId);
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
     * Get Max LogSegment Size in Bytes.
     *
     * @return max logsegment size in bytes.
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
     */
    public DistributedLogConfiguration setMaxLogSegmentBytes(long maxBytes) {
        setProperty(BKDL_MAX_LOGSEGMENT_BYTES, maxBytes);
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
     * Set if we should enable row aware ensemble placement
     *
     * @param enableRowAwareEnsemblePlacement
     *          enableRowAwareEnsemblePlacement
     */
    public DistributedLogConfiguration setRowAwareEnsemblePlacementEnabled(boolean enableRowAwareEnsemblePlacement) {
        setProperty(BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT, enableRowAwareEnsemblePlacement);
        return this;
    }

    /**
     * Get if row aware ensemble placement is enabled
     *
     * @return if row aware ensemble placement is enabled
     */
    public boolean getRowAwareEnsemblePlacementEnabled() {
        return getBoolean(BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT, BKDL_ROW_AWARE_ENSEMBLE_PLACEMENT_DEFAULT);
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
     * Set the number of dedicated readahead worker threads used by distributedlog manager factory.
     * If it is set to zero or any negative number, it would use the normal executor for readahead.
     *
     * @param numWorkerThreads
     *          number of dedicated readahead worker threads.
     * @return configuration
     */
    public DistributedLogConfiguration setNumReadAheadWorkerThreads(int numWorkerThreads) {
        setProperty(BKDL_NUM_READAHEAD_WORKER_THREADS, numWorkerThreads);
        return this;
    }

    /**
     * Get the number of dedicated readahead worker threads used by distributedlog manager factory.
     *
     * @return number of dedicated readahead worker threads.
     */
    public int getNumReadAheadWorkerThreads() {
        return getInt(BKDL_NUM_READAHEAD_WORKER_THREADS, 0);
    }

    /**
     * Set the number of lock state threads used by distributedlog manager factory.
     *
     * @param numLockStateThreads
     *          number of lock state threads used by distributedlog manager factory.
     * @return configuration
     */
    public DistributedLogConfiguration setNumLockStateThreads(int numLockStateThreads) {
        setProperty(BKDL_NUM_LOCKSTATE_THREADS, numLockStateThreads);
        return this;
    }

    /**
     * Get the number of lock state threads used by distributedlog manager factory.
     *
     * @return number of lock state threads used by distributedlog manager factory.
     */
    public int getNumLockStateThreads() {
        return getInt(BKDL_NUM_LOCKSTATE_THREADS, 1);
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
     * Get if we should ignore truncation status when reading the records
     *
     * @return if we should ignore truncation status
     */
    public boolean getIgnoreTruncationStatus() {
        return getBoolean(BKDL_READER_IGNORE_TRUNCATION_STATUS, BKDL_READER_IGNORE_TRUNCATION_STATUS_DEFAULT);
    }

    /**
     * Set if we should alert when reader is positioned on a truncated segment
     *
     * @param alertWhenPositioningOnTruncated
     *          if we should alert when reader is positioned on a truncated segment
     */
    public DistributedLogConfiguration setAlertWhenPositioningOnTruncated(boolean alertWhenPositioningOnTruncated) {
        setProperty(BKDL_READER_ALERT_POSITION_ON_TRUNCATED, alertWhenPositioningOnTruncated);
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

    /**
     * Get whether position gap detection for reader enabled.
     * @return whether position gap detection for reader enabled.
     */
    public boolean getPositionGapDetectionEnabled() {
        return getBoolean(BKDL_READER_POSITION_GAP_DETECTION_ENABLED, BKDL_READER_POSITION_GAP_DETECTION_ENABLED_DEFAULT);
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
     * Get zookeeper access rate limit.
     *
     * @return zookeeper access rate limit.
     */
    public double getZKRequestRateLimit() {
        return this.getDouble(BKDL_ZK_REQUEST_RATE_LIMIT, BKDL_ZK_REQUEST_RATE_LIMIT_DEFAULT);
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
     * Set zookeeper access rate limit
     *
     * @param requestRateLimit
     *          zookeeper access rate limit
     */
    public DistributedLogConfiguration setZKRequestRateLimit(double requestRateLimit) {
        setProperty(BKDL_ZK_REQUEST_RATE_LIMIT, requestRateLimit);
        return this;
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
     * @return periodic flush frequency
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
     * Get minimum delay between immediate flushes in milliseconds
     *
     * @return minimum delay between immediate flushes in milliseconds
     */
    public int getMinDelayBetweenImmediateFlushMs() {
        return this.getInt(BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS, BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS_DEFAULT);
    }

    /**
     * Set minimum delay between immediate flushes in milliseconds
     *
     * @param minDelayMs minimum delay between immediate flushes in milliseconds.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setMinDelayBetweenImmediateFlushMs(int minDelayMs) {
        setProperty(BKDL_MINIMUM_DELAY_BETWEEN_IMMEDIATE_FLUSH_MILLISECONDS, minDelayMs);
        return this;
    }

    /**
     * Is immediate flush enabled
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
     */
    public DistributedLogConfiguration setImmediateFlushEnabled(boolean enabled) {
        setProperty(BKDL_ENABLE_IMMEDIATE_FLUSH, enabled);
        return this;
    }

    /**
     * Is truncation managed explicitly by the application. If this is set then
     * time based retention is only a hint to perform deferred cleanup. However
     * we never remove a segment that has not been already marked truncated
     *
     * @return whether truncation managed explicitly by the application
     */
    public boolean getExplicitTruncationByApplication() {
        return getBoolean(BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION, BKDL_EXPLICIT_TRUNCATION_BY_APPLICATION_DEFAULT);
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
     * Get the wait time between successive attempts to poll for new log records
     *
     * @return read ahead wait time
     */
    public int getReadAheadWaitTime() {
        return this.getInt(BKDL_READAHEAD_WAITTIME, BKDL_READAHEAD_WAITTIME_DEFAULT);
    }

    /**
     * Set the wait time between successive attempts to poll for new log records
     *
     * @param readAheadWaitTime read ahead wait time
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setReadAheadWaitTime(int readAheadWaitTime) {
        setProperty(BKDL_READAHEAD_WAITTIME, readAheadWaitTime);
        return this;
    }

    /**
     * Get the wait time if it reaches end of stream and <b>there isn't any inprogress logsegment in the stream</b>, in millis.
     *
     * @see #setReadAheadWaitTimeOnEndOfStream(int)
     * @return the wait time if it reaches end of stream and there isn't any inprogress logsegment in the stream, in millis.
     */
    public int getReadAheadWaitTimeOnEndOfStream() {
        return this.getInt(BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM, BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM_DEFAULT);
    }

    /**
     * Set the wait time that would be used for readahead to backoff polling logsegments from zookeeper when it reaches end of stream
     * and there isn't any inprogress logsegment in the stream. The unit is millis.
     *
     * @param waitTime
     *          wait time that readahead used to backoff when reaching end of stream.
     * @return distributedlog configuration
     */
    public DistributedLogConfiguration setReadAheadWaitTimeOnEndOfStream(int waitTime) {
        setProperty(BKDL_READAHEAD_WAITTIME_ON_ENDOFSTREAM, waitTime);
        return this;
    }

    /**
     * If readahead keeps receiving {@link org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException} on
     * reading last add confirmed in given period, it would stop polling last add confirmed and re-initialize the ledger
     * handle and retry. The threshold is specified in milliseconds.
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
     * Get the long poll time out for read last add confirmed requests
     *
     * @return long poll timeout
     */
    public int getReadLACLongPollTimeout() {
        return this.getInt(BKDL_READLACLONGPOLL_TIMEOUT, BKDL_READLACLONGPOLL_TIMEOUT_DEFAULT);
    }

    /**
     * Set the long poll time out for read last add confirmed requests
     *
     * @param readAheadLongPollTimeout long poll timeout
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setReadLACLongPollTimeout(int readAheadLongPollTimeout) {
        setProperty(BKDL_READLACLONGPOLL_TIMEOUT, readAheadLongPollTimeout);
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
     * Get lock reacquire timeout
     *
     * @return lock reacquire timeout
     */
    public long getLockReacquireTimeoutMilliSeconds() {
        return this.getLong(BKDL_LOCK_REACQUIRE_TIMEOUT, BKDL_LOCK_REACQUIRE_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set lock reacquire timeout
     *
     * @param lockReacquireTimeout lock reacquire timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setLockReacquireTimeoutMilliSeconds(long lockReacquireTimeout) {
        setProperty(BKDL_LOCK_REACQUIRE_TIMEOUT, lockReacquireTimeout);
        return this;
    }

    /**
     * Get lock internal operation timeout
     *
     * @return lock internal operation timeout
     */
    public long getLockOpTimeoutMilliSeconds() {
        return this.getLong(BKDL_LOCK_OP_TIMEOUT, BKDL_LOCK_OP_TIMEOUT_DEFAULT) * 1000;
    }

    /**
     * Set lock internal operation timeout
     *
     * @param lockOpTimeout lock internal operation timeout.
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setLockOpTimeoutMilliSeconds(long lockOpTimeout) {
        setProperty(BKDL_LOCK_OP_TIMEOUT, lockOpTimeout);
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
     * Get zookeeper access rate limit for zookeeper client used in bookkeeper client.
     * @return zookeeper access rate limit for zookeeper client used in bookkeeper client.
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
     */
    public DistributedLogConfiguration setBKClientZKRequestRateLimit(double rateLimit) {
        setProperty(BKDL_BKCLIENT_ZK_REQUEST_RATE_LIMIT, rateLimit);
        return this;
    }

    /**
     * Get num of retries for zookeeper client.
     *
     * @return num of retries of zookeeper client.
     */
    public int getBKClientZKNumRetries() {
        return this.getInt(BKDL_BKCLIENT_ZK_NUM_RETRIES, BKDL_BKCLIENT_ZK_NUM_RETRIES_DEFAULT);
    }

    /**
     * Get the start backoff time of zookeeper operation retries, in seconds.
     *
     * @return start backoff time of zookeeper operation retries.
     */
    public int getBKClientZKRetryBackoffStartMillis() {
        return this.getInt(BKDL_BKCLIENT_ZK_RETRY_BACKOFF_START_MILLIS,
                           BKDL_BKCLIENT_ZK_RETRY_BACKOFF_START_MILLIS_DEFAULT);
    }

    /**
     * Get the max backoff time of zookeeper operation retries, in seconds.
     *
     * @return max backoff time of zookeeper operation retries.
     */
    public int getBKClientZKRetryBackoffMaxMillis() {
        return this.getInt(BKDL_BKCLIENT_ZK_RETRY_BACKOFF_MAX_MILLIS,
                BKDL_BKCLIENT_ZK_RETRY_BACKOFF_MAX_MILLIS_DEFAULT);
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
     * Get DL ledger metadata output layout version
     *
     * @return layout version
     */
    public int getDLLedgerMetadataLayoutVersion() {
        return this.getInt(BKDL_LEDGER_METADATA_LAYOUT_VERSION, BKDL_LEDGER_METADATA_LAYOUT_VERSION_DEFAULT);
    }

    /**
     * Set DL ledger metadata output layout version
     *
     * @param layoutVersion layout version
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setDLLedgerMetadataLayoutVersion(int layoutVersion) throws IllegalArgumentException {
        if ((layoutVersion <= 0) ||
            (layoutVersion > LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION)) {
            // Incorrect version specified
            throw new IllegalArgumentException("Incorrect value for ledger metadata layout version");
        }
        setProperty(BKDL_LEDGER_METADATA_LAYOUT_VERSION, layoutVersion);
        return this;
    }

    /**
     * Get the setting for whether we should enforce the min ledger metadata version check
     *
     * @return whether we should enforce the min ledger metadata version check
     */
    public boolean getDLLedgerMetadataSkipMinVersionCheck() {
        return this.getBoolean(BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK, BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK_DEFAULT);
    }

    /**
     * Set if we should skip the enforcement of min ledger metadata version
     *
     * @param skipMinVersionCheck whether we should enforce the min ledger metadata version check
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setDLLedgerMetadataSkipMinVersionCheck(boolean skipMinVersionCheck) throws IllegalArgumentException {
        setProperty(BKDL_LEDGER_METADATA_SKIP_MIN_VERSION_CHECK, skipMinVersionCheck);
        return this;
    }

    /**
     * Get the value at which ledger sequence number should start for streams that are being
     * upgraded and did not have ledger sequence number to start with or for newly created
     * streams
     *
     * @return first ledger sequence number
     */
    public long getFirstLedgerSequenceNumber() {
        return this.getLong(BKDL_FIRST_LEDGER_SEQUENCE_NUMBER, BKDL_FIRST_LEDGER_SEQUENCE_NUMBER_DEFAULT);
    }

    /**
     * Set the value at which ledger sequence number should start for streams that are being
     * upgraded and did not have ledger sequence number to start with or for newly created
     * streams
     *
     * @param firstLedgerSequenceNumber first ledger sequence number
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setFirstLedgerSequenceNumber(long firstLedgerSequenceNumber) throws IllegalArgumentException {
        if (firstLedgerSequenceNumber <= 0) {
            // Incorrect ledger sequence number specified
            throw new IllegalArgumentException("Incorrect value for ledger sequence number");
        }
        setProperty(BKDL_FIRST_LEDGER_SEQUENCE_NUMBER, firstLedgerSequenceNumber);
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
     * Whether sanity check txn id.
     *
     * @return true if should check txn id with max txn id.
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
     */
    public DistributedLogConfiguration setSanityCheckTxnID(boolean enabled) {
        setProperty(BKDL_MAXID_SANITYCHECK, enabled);
        return this;
    }

    /**
     * Whether to enable ledger allocator pool or not.
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
     */
    public DistributedLogConfiguration setEnableLedgerAllocatorPool(boolean enabled) {
        setProperty(BKDL_ENABLE_LEDGER_ALLOCATOR_POOL, enabled);
        return this;
    }

    /**
     * The path of ledger allocator pool.
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
     */
    public DistributedLogConfiguration setLedgerAllocatorPoolPath(String path) {
        setProperty(BKDL_LEDGER_ALLOCATOR_POOL_PATH, path);
        return this;
    }

    /**
     * The name of ledger allocator pool.
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
     * Core size of ledger allocator pool.
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
     */
    public DistributedLogConfiguration setLedgerAllocatorPoolCoreSize(int poolSize) {
        setProperty(BKDL_LEDGER_ALLOCATOR_POOL_CORE_SIZE, poolSize);
        return this;
    }

    /**
     * Whether to create stream if not exists.
     *
     * @return true if it is abled to create stream if not exists.
     */
    public boolean getCreateStreamIfNotExists() {
        return getBoolean(BKDL_CREATE_STREAM_IF_NOT_EXISTS, BKDL_CREATE_STREAM_IF_NOT_EXISTS_DEFAULT);
    }

    /**
     * Enable/Disable creating stream if not exists.
     *
     * @param enabled
     *          enable/disable sanity check txn id.
     * @return distributed log configuration.
     */
    public DistributedLogConfiguration setCreateStreamIfNotExists(boolean enabled) {
        setProperty(BKDL_CREATE_STREAM_IF_NOT_EXISTS, enabled);
        return this;
    }
    /*
     * Get the time in milliseconds as the threshold for when an idle reader should throw errors
     *
     * @return if record counts should be persisted
     */
    public int getReaderIdleErrorThresholdMillis() {
        return getInt(BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS, BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT);
    }

    /**
     * Set the time in milliseconds as the threshold for when an idle reader should throw errors
     *
     * @param warnThreshold time after which we should throw idle reader errors
     * @return distributed log configuration
     */
    public DistributedLogConfiguration setReaderIdleErrorThresholdMillis(int warnThreshold) {
        setProperty(BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS, warnThreshold);
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
     * Get log segment rolling concurrency.
     *
     * @return log segment rolling concurrency.
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
     */
    public DistributedLogConfiguration setLogSegmentRollingConcurrency(int concurrency) {
        setProperty(BKDL_LOGSEGMENT_ROLLING_CONCURRENCY, concurrency);
        return this;
    }

    /**
     * Could encode region id in version?
     *
     * @return whether to encode region id in version.
     */
    public boolean getEncodeRegionIDInVersion() {
        return getBoolean(BKDL_ENCODE_REGION_ID_IN_VERSION, BKDL_ENCODE_REGION_ID_IN_VERSION_DEFAULT);
    }

    /**
     * Enable/Disable encoding region id in version.
     *
     * @param enabled
     *          flag to enable/disable encoding region id in version.
     * @return configuration instance.
     */
    public DistributedLogConfiguration setEncodeRegionIDInVersion(boolean enabled) {
        setProperty(BKDL_ENCODE_REGION_ID_IN_VERSION, enabled);
        return this;
    }

    /**
     * Get log segment name version.
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
     */
    public DistributedLogConfiguration setLogSegmentNameVersion(int version) {
        setProperty(BKDL_LOGSEGMENT_NAME_VERSION, version);
        return this;
    }


    /**
     * Should read ahead use long polling or piggyback for read last confirmed
     *
     * @return whether read ahead should use long polling or piggyback for read last confirmed.
     */
    public int getReadLACOption() {
        return getInt(BKDL_READLAC_OPTION, BKDL_READLAC_OPTION_DEFAULT);
    }

    /**
     * Set the method that read-ahead's should use to get read last confirmed.
     *
     * @param option
     *          flag to set the read-ahead's option for read last confirmed.
     * @return configuration instance.
     */
    public DistributedLogConfiguration setReadLACOption(int option) {
        setProperty(BKDL_READLAC_OPTION, option);
        return this;
    }

    /**
     * Get mapping used to override the region mapping derived by the default resolver.
     *
     * @return dns resolver overrides.
     */
    public String getBkDNSResolverOverrides() {
        return getString(BKDL_BK_DNS_RESOLVER_OVERRIDES, BKDL_BK_DNS_RESOLVER_OVERRIDES_DEFAULT);
    }

    /**
     * Set mapping used to override the region mapping derived by the default resolver
     *
     * @param overrides
     *          dns resolver overrides
     * @return dl configuration.
     */
    public DistributedLogConfiguration setBkDNSResolverOverrides(String overrides) {
        setProperty(BKDL_BK_DNS_RESOLVER_OVERRIDES, overrides);
        return this;
    }

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
     * @return flag to enable per stream stat.
     */
    public boolean getEnablePerStreamStat() {
        return getBoolean(BKDL_ENABLE_PERSTREAM_STAT, BKDL_ENABLE_PERSTREAM_STAT_DEFAULT);
    }

    /**
     * Set the flag to enable per stream stat or not.
     *
     * @param enabled
     *          flag to enable/disable per stream stat.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setEnablePerStreamStat(boolean enabled) {
        setProperty(BKDL_ENABLE_PERSTREAM_STAT, enabled);
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

    /**
     * Get the per stream outstanding write limit for dl.
     *
     * @return the per stream outstanding write limit for dl
     */
    public int getPerWriterOutstandingWriteLimit() {
        return getInt(BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT, BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT_DEFAULT);
    }

    /**
     * Set the per stream outstanding write limit for dl.
     *
     * @param limit
     *          per stream outstanding write limit for dl
     * @return dl configuration
     */
    public DistributedLogConfiguration setPerWriterOutstandingWriteLimit(int limit) {
        setProperty(BKDL_PER_WRITER_OUTSTANDING_WRITE_LIMIT, limit);
        return this;
    }

    /**
     * Get the global write limit for dl.
     *
     * @return the global write limit for dl
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
     */
    public DistributedLogConfiguration setGlobalOutstandingWriteLimit(int limit) {
        setProperty(BKDL_GLOBAL_OUTSTANDING_WRITE_LIMIT, limit);
        return this;
    }

    /**
     * Whether to darkmode outstanding writes limit.
     *
     * @return flag to darmkode pending write limit.
     */
    public boolean getOutstandingWriteLimitDarkmode() {
        return getBoolean(BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE, BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE_DEFAULT);
    }

    /**
     * Set the flag to darkmode outstanding writes limit.
     *
     * @param darkmoded
     *          flag to darmkode pending write limit
     * @return dl configuration.
     */
    public DistributedLogConfiguration setOutstandingWriteLimitDarkmode(boolean darkmoded) {
        setProperty(BKDL_OUTSTANDING_WRITE_LIMIT_DARKMODE, darkmoded);
        return this;
    }

    /**
     * Whether to fail immediately if the stream is not ready rather than queueing the request.
     *
     * @return should failfast.
     */
    public boolean getFailFastOnStreamNotReady() {
        return getBoolean(BKDL_FAILFAST_ON_STREAM_NOT_READY, BKDL_FAILFAST_ON_STREAM_NOT_READY_DEFAULT);
    }

    /**
     * Set the failfast on stream not ready flag.
     *
     * @param failFastOnStreamNotReady
     *        set failfast flag
     * @return dl configuration.
     */
    public DistributedLogConfiguration setFailFastOnStreamNotReady(boolean failFastOnStreamNotReady) {
        setProperty(BKDL_FAILFAST_ON_STREAM_NOT_READY, failFastOnStreamNotReady);
        return this;
    }

    /**
     * The compression type to use while sending data to bookkeeper.
     * @return
     */
    public String getCompressionType() {
        return getString(BKDL_COMPRESSION_TYPE, BKDL_COMPRESSION_TYPE_DEFAULT);
    }

    /**
     * Set the compression type to use while sending data to bookkeeper.
     * @param compressionType
     *          CompressionCodec.Type     String to use (see CompressionUtils)
     *          ---------------------     ------------------------------------
     *                  NONE               none
     *                  LZ4                lz4
     *                  UNKNOWN            any other instance of String.class
     * @return
     */
    public DistributedLogConfiguration setCompressionType(String compressionType) {
        Preconditions.checkArgument(null != compressionType && !compressionType.isEmpty());
        setProperty(BKDL_COMPRESSION_TYPE, compressionType);
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

    /**
     * Get timeout for stream op execution in proxy layer. 0 disables timeout.
     *
     * @return timeout for stream operation in proxy layer.
     */
    public long getServiceTimeoutMs() {
        return getLong(BKDL_SERVICE_TIMEOUT_MS, BKDL_SERVICE_TIMEOUT_MS_DEFAULT);
    }

    /**
     * Set timeout for stream op execution in proxy layer. 0 disables timeout.
     *
     * @param timeoutMs
     *          timeout for stream operation in proxy layer.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setServiceTimeoutMs(long timeoutMs) {
        setProperty(BKDL_SERVICE_TIMEOUT_MS, timeoutMs);
        return this;
    }

    /**
     * After service timeout, how long should stream be kept in cache in probationary state in order
     * to prevent reacquire. In millisec.
     *
     * @return stream probation timeout in ms.
     */
    public long getStreamProbationTimeoutMs() {
        return getLong(BKDL_STREAM_PROBATION_TIMEOUT_MS, BKDL_STREAM_PROBATION_TIMEOUT_MS_DEFAULT);
    }

    /**
     * After service timeout, how long should stream be kept in cache in probationary state in order
     * to prevent reacquire. In millisec.
     *
     * @param stream probation timeout in ms.
     */
    public DistributedLogConfiguration setStreamProbationTimeoutMs(long timeoutMs) {
        setProperty(BKDL_STREAM_PROBATION_TIMEOUT_MS, timeoutMs);
        return this;
    }

    /**
     * Get timeout for shutting down schedulers in dl manager.
     *
     * @return timeout for shutting down schedulers in dl manager.
     */
    public int getSchedulerShutdownTimeoutMs() {
        return getInt(BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS, BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS_DEFAULT);
    }

    /**
     * Set timeout for shutting down schedulers in dl manager.
     *
     * @param timeoutMs
     *         timeout for shutting down schedulers in dl manager.
     * @return dl configuration.
     */
    public DistributedLogConfiguration setSchedulerShutdownTimeoutMs(int timeoutMs) {
        setProperty(BKDL_SCHEDULER_SHUTDOWN_TIMEOUT_MS, timeoutMs);
        return this;
    }

    /**
     * Get feature provider class.
     *
     * @return feature provider class.
     * @throws ConfigurationException
     */
    public Class<? extends FeatureProvider> getFeatureProviderClass()
            throws ConfigurationException {
        return ReflectionUtils.getClass(this, BKDL_FEATURE_PROVIDER_CLASS, DeciderFeatureProvider.class,
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
     * Get the base config path for decider.
     *
     * @return base config path for decider.
     */
    public String getDeciderBaseConfigPath() {
        return getString(BKDL_DECIDER_BASE_CONFIG_PATH, BKDL_DECIDER_BASE_CONFIG_PATH_DEFAULT);
    }

    /**
     * Set the base config path for decider.
     *
     * @param configPath
     *          base config path for decider.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setDeciderBaseConfigPath(String configPath) {
        setProperty(BKDL_DECIDER_BASE_CONFIG_PATH, configPath);
        return this;
    }

    /**
     * Get the overlay config path for decider.
     *
     * @return overlay config path for decider.
     */
    public String getDeciderOverlayConfigPath() {
        return getString(BKDL_DECIDER_OVERLAY_CONFIG_PATH, BKDL_DECIDER_OVERLAY_CONFIG_PATH_DEFAULT);
    }

    /**
     * Set the overlay config path for decider.
     *
     * @param configPath
     *          overlay config path for decider.
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setDeciderOverlayConfigPath(String configPath) {
        setProperty(BKDL_DECIDER_OVERLAY_CONFIG_PATH, configPath);
        return this;
    }

    /**
     * Get the decider environment.
     *
     * @return decider environment
     */
    public String getDeciderEnvironment() {
        return getString(BKDL_DECIDER_ENVIRONMENT, BKDL_DECIDER_ENVIRONMENT_DEFAULT);
    }

    /**
     * Set the decider environment.
     *
     * @param environment decider environment
     * @return distributedlog configuration.
     */
    public DistributedLogConfiguration setDeciderEnvironment(String environment) {
        setProperty(BKDL_DECIDER_ENVIRONMENT, environment);
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
    public DistributedLogConfiguration setDynamicConfigReloadIntervalSec(String intervalSec) {
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

    /**
     * Whether to use daemon thread for DL threads.
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
     */
    public DistributedLogConfiguration setUseDaemonThread(boolean daemon) {
        setProperty(BKDL_USE_DAEMON_THREAD, daemon);
        return this;
    }

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
}
