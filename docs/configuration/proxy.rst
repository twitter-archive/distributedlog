Write Proxy Configuration
=========================

This section describes the configuration settings used by DistributedLog Write Proxy.

All the server related settings are managed in `ServerConfiguration`. Similar as `DistributedLogConfiguration`,
it is also a properties based configuration, which extends from Apache commons `CompositeConfiguration`. All
server related settings are in lower case and use `'_'` to concat words. For example, `server_region_id` means
the region id used by the write proxy server.

Server Configuration Settings
-----------------------------

- *server_dlsn_version*: The version of serialized format of DLSN. The default value is 1. It is not recommended to change it.
- *server_region_id*: The region id used by the server to instantiate a DL namespace. The default value is `LOCAL`.
- *server_port*: The listen port of the write proxy. The default value is 0.
- *server_shard*: The shard id used by the server to identify itself. It is optional but recommended to set. For example, if
  the write proxy is running in `Apache Aurora`, you could use the instance id as the shard id. The default value is -1 (unset).
- *server_threads*: The number of threads for the executor of this server. The default value is the available processors.
- *server_enable_perstream_stat*: The flag to enable per stream stat in write proxy. It is different from `enablePerStreamStat`
  in core library. The setting here is controlling exposing the per stream stat exposed by write proxy, while `enablePerStreamStat`
  is to control expose the per stream stat exposed by the core library. It is enabled by default.
- *server_graceful_shutdown_period_ms*: The graceful shutdown period in milliseconds. The default value is 0.
- *server_service_timeout_ms*: The timeout period for the execution of a stream operation in write proxy. If it is positive,
  write proxy will timeout requests if they are taking longer time than the threshold. Otherwise, the timeout feature is disabled.
  By default, it is 0 (disabled).
- *server_stream_probation_timeout_ms*: The time period that a stream should be kept in cache in probationary state after service
  timeout, in order to prevent ownership reacquiring. The unit is milliseconds. The default value is 5 minutes.
- *stream_partition_converter_class*: The stream-to-partition convert class. The converter is used to group streams together, which
  these streams can apply same `per-stream` configuration settings or same other constraints. By default, it is an
  `IdentityStreamPartitionConverter` which doesn't group any streams.

Rate Limit Settings
~~~~~~~~~~~~~~~~~~~

This section describes the rate limit settings per write proxy.

All the rate limit settings have both `soft` and `hard` thresholds. If the throughput goes above `soft` limit,
the requests won't be rejected but just logging in the stat. But if the throughput goes above `hard` limit,
the requests would be rejected immediately.

NOTE: `bps` stands for `bytes per second`, while `rps` stands for `requests per second`.

- *bpsSoftServiceLimit*: The soft limit for bps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.
- *bpsHardServiceLimit*: The hard limit for bps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.
- *rpsSoftServiceLimit*: The soft limit for rps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.
- *rpsHardServiceLimit*: The hard limit for rps. Setting it to 0 or negative value will disable this feature.
  By default it is disabled.

There are two additional rate limiting settings that related to stream acquisitions.

- *rpsStreamAcquireServiceLimit*: The rate limit for rps. When the rps goes above this threshold, the write proxy
  will stop accepting serving new streams.
- *bpsStreamAcquireServiceLimit*: The rate limit for bps. When the bps goes above this threshold, the write proxy
  will stop accepting serving new streams.

Stream Limit Settings
~~~~~~~~~~~~~~~~~~~~~

This section describes the stream limit settings per write proxy. They are the constraints that each write proxy
will apply when deciding whether to own given streams.

- *maxAcquiredPartitionsPerProxy*: The maximum number of partitions per stream that a write proxy is allowed to
  serve. Setting it to 0 or negative value will disable this feature. By default it is unlimited.
- *maxCachedPartitionsPerProxy*: The maximum number of partitions per stream that a write proxy is allowed to cache.
  Setting it to 0 or negative value will disable this feature. By default it is unlimited.
