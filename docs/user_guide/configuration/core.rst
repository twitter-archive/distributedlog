---
layout: default

# Sub-level navigation
sub-nav-group: user-guide
sub-nav-parent: configuration
sub-nav-id: core-library-configuration
sub-nav-pos: 1
sub-nav-title: Core Library Configuration
---

.. contents:: Core Library Configuration

Core Library Configuration
==========================

This section describes the configuration settings used by DistributedLog Core Library.

All the core library settings are managed in `DistributedLogConfiguration`, which is
basically a properties based configuration, which extends from Apache commons
`CompositeConfiguration`. All the DL settings are in camel case and prefixed with a
meaningful component name. For example, `zkSessionTimeoutSeconds` means the session timeout
for component `zk` in seconds.

The default distributedlog configuration is constructed by instantiating an instance
of `DistributedLogConfiguration`. This distributedlog configuration will automatically load
the settings that specified via `SystemConfiguration`.

::

    DistributedLogConfiguration conf = new DistributedLogConfiguration();

The recommended way is to load configuration from URL that points to a configuration file
(`#loadConf(URL)`).

::

    String configFile = "/path/to/distributedlog/conf/file";
    DistributedLogConfiguration conf = new DistributedLogConfiguration();
    conf.loadConf(new File(configFile).toURI().toURL());

ZooKeeper Settings
------------------

A distributedlog namespace usually creates two zookeeper client instances: one is used
for DL metadata operations, while the other one is used by bookkeeper. All the zookeeper
clients are *retryable* clients, which they would reconnect when session is expired.

DL ZooKeeper Settings
~~~~~~~~~~~~~~~~~~~~~

- *zkSessionTimeoutSeconds*: ZooKeeper session timeout, in seconds. Default is 30 seconds.
- *zkNumRetries*: Number of retries of each zookeeper request could attempt on retryable exceptions.
  Default is 3.
- *zkRetryStartBackoffMillis*: The initial backoff time of first retry of each zookeeper request, in milliseconds.
  Default is 5000.
- *zkRetryMaxBackoffMillis*: The max backoff time of retries of each zookeeper request, in milliseconds.
  Default is 30000.
- *zkcNumRetryThreads*: The number of retry threads used by this zookeeper client. Default is 1.
- *zkRequestRateLimit*: The rate limiter is basically a guava `RateLimiter`. It is rate limiting the
  requests that sent by zookeeper client per second. If the value is non-positive, the rate limiting
  is disable. Default is 0.
- *zkAclId*: The digest id used for zookeeper ACL. If it is null, ACL is disabled. Default is null.

BK ZooKeeper Settings
~~~~~~~~~~~~~~~~~~~~~

- *bkcZKSessionTimeoutSeconds*: ZooKeeper session timeout, in seconds. Default is 30 seconds.
- *bkcZKNumRetries*: Number of retries of each zookeeper request could attempt on retryable exceptions.
  Default is 3.
- *bkcZKRetryStartBackoffMillis*: The initial backoff time of first retry of each zookeeper request, in milliseconds.
  Default is 5000.
- *bkcZKRetryMaxBackoffMillis*: The max backoff time of retries of each zookeeper request, in milliseconds.
  Default is 30000.
- *bkcZKRequestRateLimit*: The rate limiter is basically a guava `RateLimiter`. It is rate limiting the
  requests that sent by zookeeper client per second. If the value is non-positive, the rate limiting
  is disable. Default is 0.

There are a few rules to follow when optimizing the zookeeper settings:

1. In general, higher session timeout is much better than lower timeout, which will make zookeeper client
   more resilent to any network glitches.
2. A lower backoff time is better for latency, as it would trigger fast retries. But it
   could trigger retry storm if the backoff time is too low.
3. Number of retries should be tuned based on the backoff time settings and corresponding latency SLA budget.
4. BK and DL readers use zookeeper client for metadata accesses. It is recommended to have higher session timeout,
   higher number of retries and proper backoff time.
5. DL writers also use zookeeper client for ownership tracking. It is required to act quickly on network glitches.
   It is recommended to have low session timeout, low backoff time and proper number of retries.

BookKeeper Settings
-------------------

All the bookkeeper client configuration settings could be loaded via `DistributedLogConfiguration`. All of them
are prefixed with `bkc.`. For example, `bkc.zkTimeout` in distributedlog configuration will be applied as
`zkTimeout` in bookkeeper client configuration.

General Settings
~~~~~~~~~~~~~~~~

- *bkcNumIOThreads*: The number of I/O threads used by netty in bookkeeper client.
  The default value is `numWorkerThreads`.

Timer Settings
~~~~~~~~~~~~~~

- *timerTickDuration*: The tick duration in milliseconds that used for timeout
  timer in bookkeeper client. The default value is 100 milliseconds.
- *timerNumTicks*: The number of ticks that used for timeout timer in bookkeeper client.
  The default value is 1024.

Data Placement Settings
~~~~~~~~~~~~~~~~~~~~~~~

A log segment is backed by a bookkeeper `ledger`. A ledger's data is stored in an ensemble
of bookies in a stripping way. Each entry will be added in a `write-quorum` size of bookies.
The add operation will complete once it receives responses from a `ack-quorum` size of bookies.
The stripping is done in a round-robin way in bookkeeper.

For example, we configure the ensemble-size to 5, write-quorum-size to 3,
and ack-quorum-size to 2. The data will be stored in following stripping way.

::

    | entry id | bk1 | bk2 | bk3 | bk4 | bk5 |
    |     0    |  x  |  x  |  x  |     |     |
    |     1    |     |  x  |  x  |  x  |     |
    |     2    |     |     |  x  |  x  |  x  |
    |     3    |  x  |     |     |  x  |  x  |
    |     4    |  x  |  x  |     |     |  x  |
    |     5    |  x  |  x  |  x  |     |     |

We don't recommend stripping within a log segment to increase bandwidth. We'd recommend using
multiple distributedlog streams to increase bandwidth in higher level of distributedlog. so
typically the ensemble size will be set to be the same value as `write-quorum-size`.

- *bkcEnsembleSize*: The ensemble size of the log segment. The default value is 3.
- *bkcWriteQuorumSize*: The write quorum size of the log segment. The default value is 3.
- *bkcAckQuorumSize*: The ack quorumm size of the log segment. The default value is 2.

DNS Resolver Settings
+++++++++++++++++++++

DistributedLog uses bookkeeper's `rack-aware` data placement policy on placing data across
bookkeeper nodes. The `rack-aware` data placement uses a DNS resolver to resolve a bookie
address into a network location and then use those locations to build the network topology.

There are two built-in DNS resolvers in DistributedLog:

1. *DNSResolverForRacks*: It resolves domain name like `(region)-(rack)-xxx-xxx.*` to
   network location `/(region)/(rack)`. If resolution failed, it returns `/default-region/default-rack`.
2. *DNSResolverForRows*: It resolves domain name like `(region)-(row)xx-xxx-xxx.*` to
   network location `/(region)/(row)`. If resolution failed, it returns `/default-region/default-row`.

The DNS resolver could be loaded by reflection via `bkEnsemblePlacementDnsResolverClass`.

`(region)` could be overrided in a configured `dnsResolverOverrides`. For example, if the
host name is `(regionA)-(row1)-xx-yyy`, it would be resolved to `/regionA/row1` without any
overrides. If the specified overrides is `(regionA)-(row1)-xx-yyy:regionB`,
the resolved network location would be `/regionB/row1`. Allowing overriding region provides
the optimization hits to bookkeeper if two `logical` regions are in same or close locations.

- *bkEnsemblePlacementDnsResolverClass*: The DNS resolver class for bookkeeper rack-aware ensemble placement.
  The default value is `DNSResolverForRacks`.
- *bkRowAwareEnsemblePlacement*: A flag indicates whether `DNSResolverForRows` should be used.
  If enabled, `DNSResolverForRows` will be used for DNS resolution in rack-aware placement policy.
  Otherwise, it would use the DNS resolver configured by `bkEnsemblePlacementDnsResolverClass`.
- *dnsResolverOverrides*: The mapping used to override the region mapping derived by the DNS resolver.
  The value is a string of pairs of host-region mappings (`host:region`) separated by semicolon.
  By default it is empty string.

Namespace Configuration Settings
--------------------------------

This section lists all the general settings used by `DistributedLogNamespace`.

Executor Settings
~~~~~~~~~~~~~~~~~

- *numWorkerThreads*: The number of worker threads used by the namespace instance.
  The default value is the number of available processors.
- *numReadAheadWorkerThreads*: The number of dedicated readahead worker treads used
  by the namespace instance. If it is non-positive, it would share the same executor
  for readahead. Otherwise, it would create a dedicated executor for readahead.
  The default value is 0.
- *numLockStateThreads*: The number of lock state threads used by the namespace instance.
  The default value is 1.
- *schedulerShutdownTimeoutMs*: The timeout value in milliseconds, for shutting down
  schedulers in the namespace instance. The default value is 5000ms.
- *useDaemonThread*: The flag whether to use daemon thread for DL executor threads.
  The default value is false.

Metadata Settings
~~~~~~~~~~~~~~~~~

The log segment metadata is serialized into a string of content with a version. The version in log segment
metadata allows us evolving changes to metadata. All the versions supported by distributedlog right now
are listed in the below table.

+--------+-----------------------------------------------------------------------------------+
|version |description                                                                        |
+========+===================================================================================+
|   0    |Invalid version number.                                                            |
+--------+-----------------------------------------------------------------------------------+
|   1    |Basic version number.                                                              |
|        |Inprogress: start tx id, ledger id, region id                                      |
|        |Completed: start/end tx id, ledger id, region id, record count and completion time |
+--------+-----------------------------------------------------------------------------------+
|   2    |Introduced LSSN (LogSegment Sequence Number)                                       |
+--------+-----------------------------------------------------------------------------------+
|   3    |Introduced Partial Truncated and Truncated status.                                 |
|        |A min active (entry_id, slot_id) pair is recorded in completed log segment         |
|        |metadata.                                                                          |
+--------+-----------------------------------------------------------------------------------+
|   4    |Introduced Enveloped Entry Stucture. None & LZ4 compression codec introduced.      |
+--------+-----------------------------------------------------------------------------------+
|   5    |Introduced Sequence Id.                                                            |
+--------+-----------------------------------------------------------------------------------+

A general rule for log segment metadata upgrade is described as below. For example, we are upgrading
from version *X* to version *X+1*.

1. Upgrade the readers before upgrading writers. So the readers are able to recognize the log segments of version *X+1*.
2. Upgrade the writers with the new binary of version *X+1* only. Keep the configuration `ledgerMetadataLayoutVersion` unchanged - still in version *X*.
3. Once all the writers are running in same binary of version *X+1*. Update writers again with `ledgerMetadataLayoutVersion` set to version *X+1*.

**Available Settings**

- *ledgerMetadataLayoutVersion*: The logsegment metadata layout version. The default value is 5. Apply for `writers` only.
- *ledgerMetadataSkipMinVersionCheck*: The flag indicates whether DL should enforce minimum log segment metadata vesion check.
  If it is true, DL will skip the checking and read the log segment metadata if it could recognize. Otherwise, it would fail
  the read if the log segment's metadata version is less than the version that DL supports. By default, it is disabled.
- *firstLogsegmentSequenceNumber*: The first log segment sequence number to start with for a stream. The default value is 1.
  The setting is only applied for writers, and only when upgrading metadata from version `1` to version `2`.
  In this upgrade, we need to update old log segments to add ledger sequence number, once the writers start generating
  new log segments with new version starting from this `firstLogSegmentSequenceNumber`.
- *maxIdSanityCheck*: The flag indicates whether DL should do sanity check on transaction id. If it is enabled, DL will throw
  `TransactionIdOutOfOrderException` when it received a smaller transaction id than current maximum transaction id. By default,
  it is enabled.
- *encodeRegionIDInVersion*: The flag indicates whether DL should encode region id into log segment metadata. In a global replicated
  log, the log segments can be created in different regions. The region id in log segment metadata would help figuring out what
  region that a log segment is created. The region id in log segment metadata would help for monitoring and troubleshooting.
  By default, it is disabled.

Namespace Settings
~~~~~~~~~~~~~~~~~~

- *federatedNamespaceEnabled*: The flag indicates whether DL should use federated namespace. By default, it is disabled.
- *federatedMaxLogsPerSubnamespace*: The maximum number of log stream per sub namespace in a federated namespace. By default, it is 15000
- *federatedCheckExistenceWhenCacheMiss*: The flag indicates whether to check the existence of a log stream in zookeeper or not,
  if querying the local cache of the federated namespace missed.

Writer Configuration Settings
-----------------------------

General Settings
~~~~~~~~~~~~~~~~

- *createStreamIfNotExists*: The flag indicates whether to create a log stream if it doesn't exist. By default, it is true.
- *compressionType*: The compression type used when enveloping the output buffer. The available compression types are
  `none` and `lz4`. By default, it is `none` - no compression.
- *failFastOnStreamNotReady*: The flag indicates whether to fail immediately if the stream is not ready rather than enqueueing
  the request. A log stream is considered as `not-ready` when it is either initializing the log stream or rolling a new log
  segment. If this is enabled, DL would fail the write request immediately when the stream isn't ready. Otherwise, it would
  enqueue the request and wait for the stream become ready. Please consider turning it on for the use cases that could retry
  writing to other log streams, which it would result in fast failure hence client could retry other streams immediately.
  By default, it is disabled.
- *disableRollingOnLogSegmentError*: The flag to disable rolling log segment when encountered error. By default, it is true.

Durability Settings
~~~~~~~~~~~~~~~~~~~

- *isDurableWriteEnabled*: The flag indicates whether durable write is enabled. By default it is true.

Transmit Settings
~~~~~~~~~~~~~~~~~

DL writes the log records into a transmit buffer before writing to bookkeeper. The following settings control
the frequency of transmits and commits.

- *writerOutputBufferSize*: The output buffer size in bytes. Larger buffer size will result in higher compression ratio and it would reduce the entries sent to bookkeeper, use the disk bandwidth more efficiently and improve throughput. Set this setting to `0` will ask DL to transmit the data immediately, which it would achieve low latency.
- *periodicFlushFrequencyMilliSeconds*: The periodic flush frequency in milliseconds. If the setting is set to a positive value, the data in transmit buffer will be flushed in every half of the provided interval. Otherwise, the periodical flush will be disabled. For example, if this setting is set to `10` milliseconds, the data will be flushed (`transmit`) every 5 milliseconds.
- *enableImmediateFlush*: The flag to enable immediate flush a control record. It is a flag to control the period to make data visible to the readers. If this settings is true, DL would flush a control record immediately after transmitting the user data is completed. The default value is false.
- *minimumDelayBetweenImmediateFlushMilliSeconds*: The minimum delay between two immediate flushes, in milliseconds. This setting only takes effects when immediate flush is enabled. It is designed to tolerant the bursty of traffic when immediate flush is enabled, which prevents sending too many control records to the bookkeeper.

LogSegment Retention Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following settings are related to log segment retention.

- *logSegmentRetentionHours*: The log segment retention period, in hours. In other words, how long should DL keep the log segment once it is `truncated`.
- *explicitTruncationByApp*: The flag indicates that truncation is managed explicitly by the application. If this is set then time based retention only clean the log segments which are marked as `truncated`. By default it is disabled.

LogSegment Rolling Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following settings are related to log segment rolling.

- *logSegmentRollingMinutes*: The log segment rolling interval, in minutes. If the setting is set to a positive value, DL will roll
  log segments based on time. Otherwise, it will roll log segment based on size (`maxLogSegmentBytes`). The default value is 2 hours.
- *maxLogSegmentBytes*: The maximum size of a log segment, in bytes. This setting only takes effects when time based rolling is disabled.
  If it is enabled, DL will roll a new log segment when the current one reaches the provided threshold. The default value is 256MB.
- *logSegmentRollingConcurrency*: The concurrency of log segment rolling. If the value is positive, it means how many log segments
  can be rolled at the same time. Otherwise, it is unlimited. The default value is 1.

LogSegment Allocation Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A bookkeeper ledger is allocated when a DL stream is rolling into a new log segment. To reduce the latency penalty on log segment rolling,
a ledger allocator could be used for pre-allocating the ledgers for DL streams. This section describes the settings related to ledger
allocation.

- *enableLedgerAllocatorPool*: The flag indicates whether to use ledger allocator pool or not. It is disabled by default. It is recommended
  to enable on write proxy.
- *ledgerAllocatorPoolPath*: The path of the ledger allocator pool. The default value is ".allocation_pool". The allocator pool path has to
  be prefixed with `"."`. A DL namespace is allowed to have multiple allocator pool, as they will be acted independently.
- *ledgerAllocatorPoolName*: The name of the ledger allocator pool. Default value is null. It is set by write proxy on startup.
- *ledgerAllocatorPoolCoreSize*: The number of ledger allocators in the pool. The default value is 20.

Write Limit Settings
~~~~~~~~~~~~~~~~~~~~

This section describes the settings related to queue-based write limiting.

- *globalOutstandingWriteLimit*: The maximum number of outstanding writes. If this setting is set to a positive value, the global
  write limiting is enabled - when the number of outstanding writes go above the threshold, the consequent requests will be rejected
  with `OverCapacity` exceptions. Otherwise, it is disabled. The default value is 0.
- *perWriterOutstandingWriteLimit*: The maximum number of outstanding writes per writer. It is similar as `globalOutstandingWriteLimit`
  but applied per writer instance. The default value is 0.
- *outstandingWriteLimitDarkmode*: The flag indicates whether the write limiting is running in darkmode or not. If it is running in
  dark mode, the request is not rejected when it is over limit, but just record it in the stats. By default, it is in dark mode. It
  is recommended to run in dark mode to understand the traffic pattern before enabling real write limiting.

Lock Settings
~~~~~~~~~~~~~

This section describes the settings related to distributed lock used by the writers.

- *lockTimeoutSeconds*: The lock timeout in seconds. The default value is 30. If it is 0 or negative, the caller will attempt claiming
  the lock, if there is no owner, it would claim successfully, otherwise it would return immediately and throw exception to indicate
  who is the current owner.

Reader Configuration Settings
-----------------------------

General Settings
~~~~~~~~~~~~~~~~

- *readLACLongPollTimeout*: The long poll timeout for reading `LastAddConfirmed` requests, in milliseconds.
  The default value is 1 second. It is typically recommended to tune approximately with the request arrival interval. Otherwise, it would
  end up becoming unnecessary short polls.

ReadAhead Settings
~~~~~~~~~~~~~~~~~~

This section describes the settings related to readahead in DL readers.

- *enableReadAhead*: Flag to enable read ahead in DL readers. It is enabled by default.
- *readAheadMaxRecords*: The maximum number of records that will be cached in readahead cache by the DL readers. The default value
  is 10. A higher value will improve throughput but use more memory. It should be tuned properly to avoid jvm gc if the reader cannot
  keep up with the writing rate.
- *readAheadBatchSize*: The maximum number of entries that readahead worker will read in one batch. The default value is 2.
  Increase the value to increase the concurrency of reading entries from bookkeeper. It is recommended to tune to a proper value for
  catching up readers, not to exhaust bookkeeper's bandwidth.
- *readAheadWaitTimeOnEndOfStream*: The wait time if the reader reaches end of stream and there isn't any new inprogress log segment,
  in milliseconds. The default value is 10 seconds.
- *readAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis*: If readahead worker keeps receiving `NoSuchLedgerExists` exceptions
  when reading `LastAddConfirmed` in the given period, it would stop long polling `LastAddConfirmed` and re-initialize the ledger handle
  and retry. The threshold is in milliseconds. The default value is 10 seconds.

Reader Constraint Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~

This section describes the constraint settings in DL reader.

- *ignoreTruncationStatus*: The flag whether to ignore truncation status when reading the records. By default, it is false.
  The readers will not attempt to read a log segment that is marked as `Truncated` if this setting is false. It can be enabled for
  tooling and troubleshooting.
- *alertPositionOnTruncated*: The flag whether we should alert when reader is positioned on a truncated segment. By default, it is true.
  It would alert and fail the reader if it is positioned at a `Truncated` log segment when the setting is true. It can be disabled for
  tooling and troubleshooting.
- *positionGapDetectionEnabled*: The flag whether to enable position gap detection or not. This is a very strict constraint on reader,
  to prevent readers miss reading records due to any software bugs. It is enabled by default.

Idle Reader Settings
~~~~~~~~~~~~~~~~~~~~

There is a mechanism to detect idleness of readers, to prevent reader becoming stall due to any bugs.

- *readerIdleWarnThresholdMillis*: The warning threshold of the time that a reader becomes idle, in milliseconds. If a reader becomes
  idle more than the threshold, it would dump warnings in the log. The default value is 2 minutes.
- *readerIdleErrorThresholdMillis*: The error threshold of the time that a reader becomes idle, in milliseconds. If a reader becomes
  idle more than the threshold, it would throw `IdleReader` exceptions to notify applications. The default value is `Integer.MAX_VALUE`.

Scan Settings
~~~~~~~~~~~~~

- *firstNumEntriesEachPerLastRecordScan*: Number of entries to scan for first scan of reading last record. The default value is 2.
- *maxNumEntriesPerReadLastRecordScan*: Maximum number of entries for each scan to read last record. The default value is 16.

Tracing/Stats Settings
----------------------

This section describes the settings related to tracing and stats.

- *traceReadAheadDeliveryLatency*: Flag to enable tracing read ahead delivery latency. By default it is disabled.
- *metadataLatencyWarnThresholdMs*: The warn threshold of metadata access latency, in milliseconds. If a metadata operation takes
  more than the threshold, it would be logged. By default it is 1 second.
- *dataLatencyWarnThresholdMs*: The warn threshold for data access latency, in milliseconds. If a data operation takes
  more than the threshold, it would be logged. By default it is 2 seconds.
- *traceReadAheadMetadataChanges*: Flag to enable tracing the major metadata changes in readahead. If it is enabled, it will log
  the readahead metadata changes with precise timestamp, which is helpful for troubleshooting latency related issues. By default it
  is disabled.
- *enableTaskExecutionStats*: Flag to trace long running tasks and record task execution stats in the thread pools. It is disabled
  by default.
- *taskExecutionWarnTimeMicros*: The warn threshold for the task execution time, in micros. The default value is 100,000.
- *enablePerStreamStat*: Flag to enable per stream stat. By default, it is disabled.

Feature Provider Settings
-------------------------

- *featureProviderClass*: The feature provider class. The default value is `DefaultFeatureProvider`, which disable all the features
  by default.

