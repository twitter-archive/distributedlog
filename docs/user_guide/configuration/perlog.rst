---
layout: default

# Sub-level navigation
sub-nav-group: user-guide
sub-nav-parent: configuration
sub-nav-id: per-stream-configuration
sub-nav-pos: 4
sub-nav-title: Per Stream Configuration
---

.. contents:: Per Stream Configuration

Per Stream Configuration
========================

Application is allowed to override `DistributedLogConfiguration` for individual streams. This is archieved
for supplying an overrided `DistributedLogConfiguration` when opening the distributedlog manager.

::

    DistributedLogNamespace namespace = ...;
    DistributedLogConfiguration perStreamConf = new DistributeLogConfiguration();
    perStreamConf.loadConf(...); // load configuration from a per stream configuration file
    DistributedLogManager dlm = namespace.openLog("test-stream", Optional.of(perStreamConf), Optional.absent());

Dynamic Configuration
---------------------

Besides overriding normal `DistributedLogConfiguration` with per stream configuration, DistributedLog also
provides loading some configuration settings dynamically. The per stream dynamic settings are offered in
`DynamicDistributedLogConfiguration`.

File Based Dynamic Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default dynamic configuration implementation is based on properties files and reloading the file periodically.

::

    ConcurrentBaseConfiguration defaultConf = ...; // base config to fall through
    int reloadPeriod = 60; // 60 seconds
    TimeUnit reloadUnit = TimeUnit.SECOND;
    String configPath = "/path/to/per/stream/config/file";
    File configFile = new File(configPath);
    // load the fie into a properties configuration builder
    PropertiesConfigurationBuilder properties =
        new PropertiesConfigurationBuilder(configFile.toURI().toURL());
    // Construct the dynamic configuration
    DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(defaultConf);
    // add a configuration subscription to periodically reload the config from the file
    ConfigurationSubscription subscription =
        new ConfigurationSubscription(dynConf, properties, executorService, reloadPeriod, reloadUnit);

Stream Config Provider
~~~~~~~~~~~~~~~~~~~~~~

The stream config provider is designed to manage and reload dynamic configs for individual streams.

::

    String perStreamConfigDir = "/path/to/per/stream/config/dir";
    String defaultConfigPath = "/path/to/default/config/file";
    StreamPartitionConverter converter = ...;
    ScheduledExecutorService scheduler = ...;
    int reloadPeriod = 60; // 60 seconds
    TimeUnit reloadUnit = TimeUnit.SECOND;
    StreamConfigProvider provider = new ServiceStreamConfigProvider(
        perStreamConfigDir,
        defaultConfigPath,
        converter,
        scheduler,
        reloadPeriod,
        reloadUnit);

    Optional<DynamicDistributedLogConfiguration> streamConf = provider.getDynamicStreamConfig("test-stream");

- *perStreamConfigDir*: The directory contains configuration files for each stream. the file name is `<stream_name>.conf`.
- *defaultConfigPath*: The default configuration file. If there is no stream configuration file found in `perStreamConfigDir`,
  it would load the configuration from `defaultConfigPath`.
- *StreamPartitionConverter*: A converter that convert the stream names to partitions. DistributedLog doesn't provide built-in
  partitions. It leaves partition strategy to application. Application usually put the partition id in the dl stream name. So the
  converter is for group the streams and apply same configuration. For example, if application uses 3 streams and names them as
  `test-stream_000001`, `test-stream_000002` and `test-stream_000003`, a `StreamPartitionConverter` could be used to categorize them
  as partitions for stream `test-stream` and apply the configuration from file `test-stream.conf`.
- *scheduler*: The executor service that reloads configuration periodically.
- *reloadPeriod*: The reload period, in `reloadUnit`.

Available Dynamic Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~

Storage Settings
~~~~~~~~~~~~~~~~

- *logSegmentRetentionHours*: The log segment retention period, in hours. In other words, how long should DL keep the log segment once it is `truncated` or `completed`.
- *bkcEnsembleSize*: The ensemble size of the log segment. The default value is 3.
- *bkcWriteQuorumSize*: The write quorum size of the log segment. The default value is 3.
- *bkcAckQuorumSize*: The ack quorumm size of the log segment. The default value is 2.

Transmit Settings
~~~~~~~~~~~~~~~~~

- *writerOutputBufferSize*: The output buffer size in bytes. Larger buffer size will result in higher compression ratio and
  it would reduce the entries sent to bookkeeper, use the disk bandwidth more efficiently and improve throughput.
  Set this setting to `0` will ask DL to transmit the data immediately, which it would achieve low latency.

Durability Settings
~~~~~~~~~~~~~~~~~~~

- *isDurableWriteEnabled*: The flag indicates whether durable write is enabled. By default it is true.

ReadAhead Settings
~~~~~~~~~~~~~~~~~~

- *readAheadMaxRecords*: The maximum number of records that will be cached in readahead cache by the DL readers. The default value
  is 10. A higher value will improve throughput but use more memory. It should be tuned properly to avoid jvm gc if the reader cannot
  keep up with the writing rate.
- *readAheadBatchSize*: The maximum number of entries that readahead worker will read in one batch. The default value is 2.
  Increase the value to increase the concurrency of reading entries from bookkeeper. It is recommended to tune to a proper value for
  catching up readers, not to exhaust bookkeeper's bandwidth.

Rate Limit Settings
~~~~~~~~~~~~~~~~~~~

All the rate limit settings have both `soft` and `hard` thresholds. If the throughput goes above `soft` limit,
the requests won't be rejected but just logging in the stat. But if the throughput goes above `hard` limit,
the requests would be rejected immediately.

NOTE: `bps` stands for `bytes per second`, while `rps` stands for `requests per second`.

- *bpsSoftWriteLimit*: The soft limit for bps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.
- *bpsHardWriteLimit*: The hard limit for bps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.
- *rpsSoftWriteLimit*: The soft limit for rps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.
- *rpsHardWriteLimit*: The hard limit for rps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.
