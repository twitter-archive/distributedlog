Client Configuration
====================

This section describes the settings used by DistributedLog Write Proxy Client.

Different from core library, the proxy client uses a builder to configure its settings.

::

    DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
        .name("test-client")
        .clientId("test-client-id")
        .finagleNameStr("inet!localhost:8080")
        .statsReceiver(statsReceiver)
        .build();

Client Builder Settings
-----------------------

Common Settings
~~~~~~~~~~~~~~~

- *name(string)*: The name of the distributedlog client.
- *clientId(string)*: The client id used for the underneath finagle client. It is a string identifier that server will
  use to identify who are the client. So the server can book keep and optionally reject unknown clients.
- *requestTimeoutMs(int)*: The maximum time that a request could take before claiming it as failure, in milliseconds.
- *thriftmux(boolean)*: The flag to enable or disable using ThriftMux_ on the underneath channels.
- *streamFailfast(boolean)*: The flag to enable or disable failfast the requests when the server responds `stream-not-ready`.
  A stream would be treated as not ready when it is initializing or rolling log segments. The setting is only take effects
  when the write proxy also enables `failFastOnStreamNotReady`.

.. _ThriftMux: http://twitter.github.io/finagle/guide/Protocols.html#mux

Environment Settings
~~~~~~~~~~~~~~~~~~~~

DistributedLog uses finagle Names_ to identify the network locations of write proxies.
Names must be supplied when building a distributedlog client through `finagleNameStr` or
`finagleNameStrs`.

.. _Names: http://twitter.github.io/finagle/guide/Names.html

- *finagleNameStr(string)*: The finagle name to locate write proxies.
- *finagleNameStrs(string, string...)*: A list of finagle names. It is typically used by the global replicated log wherever there
  are multiple regions of write proxies. The first parameter is the finagle name of local region; while the remaining parameters
  are the finagle names for remote regions.

Redirection Settings
~~~~~~~~~~~~~~~~~~~~

DistributedLog client can redirect the requests to other write proxies when accessing a write proxy doesn't own the given stream.
This section describes the settings related to redirection.

- *redirectBackoffStartMs(int)*: The initial backoff for redirection, in milliseconds.
- *redirectBackoffMaxMs(int)*: The maximum backoff for redirection, in milliseconds.
- *maxRedirects(int)*: The maximum number of redirections that a request could take before claiming it as failure.

Channel Settings
~~~~~~~~~~~~~~~~

DistributedLog client uses FinagleClient_ to establish the connections to the write proxy. A finagle client will be
created via ClientBuilder_ for each write proxy.

.. _FinagleClient: https://twitter.github.io/finagle/guide/Clients.html

.. _ClientBuilder: http://twitter.github.io/finagle/docs/index.html#com.twitter.finagle.builder.ClientBuilder

- *clientBuilder(ClientBuilder)*: The finagle client builder to build connection to each write proxy.

Ownership Cache Settings
~~~~~~~~~~~~~~~~~~~~~~~~

DistributedLog client maintains a ownership cache locally to archieve stable deterministic request routing. Normally,
the ownership cache is propagated after identified a new owner when performing stream related operations such as write.
The client also does handshaking when initiating connections to a write proxy or periodically for fast failure detection.
During handshaking, the client also pull the latest ownership mapping from write proxies to update its local cache, which
it would help detecting ownership changes quickly, and avoid latency penalty introduced by redirection when ownership changes.

- *handshakeWithClientInfo(boolean)*: The flag to enable or disable pulling ownership mapping during handshaking.
- *periodicHandshakeIntervalMs(long)*: The periodic handshake interval in milliseconds. Every provided interval, the DL client
  will handshake with existing proxies. It would detect proxy failures during handshaking. If the interval is already greater than
  `periodicOwnershipSyncIntervalMs`, the handshake will pull the latest ownership mapping. Otherwise, it will not. The default
  value is 5 minutes. Setting it to 0 or negative number will disable this feature.
- *periodicOwnershipSyncIntervalMs(long)*: The periodic ownership sync interval, in milliseconds. If periodic handshake is
  enabled, the handshake will sync ownership if the elapsed time is greater than the sync interval.
- *streamNameRegex(string)*: The regex to match the stream names that the client cares about their ownerships.

Constraint Settings
~~~~~~~~~~~~~~~~~~~

- *checksum(boolean)*: The flag to enable/disable checksum validation on requests that sent to proxy.

Stats Settings
~~~~~~~~~~~~~~

- *statsReceiver(StatsReceiver)*: The stats receiver used for collecting stats exposed by this client.
- *streamStatsReceiver(StatsReceiver)*: The stats receiver used for collecting per stream stats exposed by this client.
