Monitoring
==========

DistributedLog uses the stats library provided by Apache BookKeeper for reporting metrics in
both the server and the client. This can be configured to report stats using pluggable stats
provider to integrate with your monitoring system.

Stats Provider
~~~~~~~~~~~~~~

`StatsProvider` is a provider that provides different kinds of stats logger for different scopes.
The provider is also responsible for reporting its managed metrics.

::

    // Create the stats provider
    StatsProvider statsProvider = ...;
    // Start the stats provider
    statsProvider.start(conf);
    // Stop the stats provider
    statsProvider.stop();

Stats Logger
____________

A scoped `StatsLogger` is a stats logger that records 3 kinds of statistics
under a given `scope`.

A `StatsLogger` could be either created by obtaining from stats provider with
the scope name:

::

    StatsProvider statsProvider = ...;
    StatsLogger statsLogger = statsProvider.scope("test-scope");

Or created by obtaining from a stats logger with a sub scope name:

::

    StatsLogger rootStatsLogger = ...;
    StatsLogger subStatsLogger = rootStatsLogger.scope("sub-scope");

All the metrics in a stats provider are managed in a hierarchical of scopes.

::

    // all stats recorded by `rootStatsLogger` are under 'root'
    StatsLogger rootStatsLogger = statsProvider.scope("root");
    // all stats recorded by 'subStatsLogger1` are under 'root/scope1'
    StatsLogger subStatsLogger1 = statsProvider.scope("scope1");
    // all stats recorded by 'subStatsLogger2` are under 'root/scope2'
    StatsLogger subStatsLogger2 = statsProvider.scope("scope2");

Counters
++++++++

A `Counter` is a cumulative metric that represents a single numerical value. A **counter**
is typically used to count requests served, tasks completed, errors occurred, etc. Counters
should not be used to expose current counts of items whose number can also go down, e.g.
the number of currently running tasks. Use `Gauges` for this use case.

To change a counter, use:

::
    
    StatsLogger statsLogger = ...;
    Counter births = statsLogger.getCounter("births");
    // increment the counter
    births.inc();
    // decrement the counter
    births.dec();
    // change the counter by delta
    births.add(-10);
    // reset the counter
    births.reset();

Gauges
++++++

A `Gauge` is a metric that represents a single numerical value that can arbitrarily go up and down.

Gauges are typically used for measured values like temperatures or current memory usage, but also
"counts" that can go up and down, like the number of running tasks.

To define a gauge, stick the following code somewhere in the initialization:

::

    final AtomicLong numPendingRequests = new AtomicLong(0L);
    StatsLogger statsLogger = ...;
    statsLogger.registerGauge(
        "num_pending_requests",
        new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }
            @Override
            public Number getSample() {
                return numPendingRequests.get();
            }
        });

The gauge must always return a numerical value when sampling.

Metrics (OpStats)
+++++++++++++++++

A `OpStats` is a set of metrics that represents the statistics of an `operation`. Those metrics
include `success` or `failure` of the operations and its distribution (also known as `Histogram`).
It is usually used for timing.

::

    StatsLogger statsLogger = ...;
    OpStatsLogger writeStats = statsLogger.getOpStatsLogger("writes");
    long writeLatency = ...;

    // register success op
    writeStats.registerSuccessfulEvent(writeLatency);

    // register failure op
    writeStats.registerFailedEvent(writeLatency);

Available Stats Providers
~~~~~~~~~~~~~~~~~~~~~~~~~

All the available stats providers are listed as below:

* Twitter Science Stats (deprecated)
* Twitter Ostrich Stats (deprecated)
* Twitter Finagle Stats
* Codahale Stats

Twitter Science Stats
_____________________

Use following dependency to enable Twitter science stats provider.

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>twitter-science-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>

Construct the stats provider for clients.

::

    StatsProvider statsProvider = new TwitterStatsProvider();
    DistributedLogConfiguration conf = ...;

    // starts the stats provider (optional)
    statsProvider.start(conf);

    // all the dl related stats are exposed under "dlog"
    StatsLogger statsLogger = statsProvider.getStatsLogger("dlog");
    DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
        .uri(...)
        .conf(conf)
        .statsLogger(statsLogger)
        .build();

    ...

    // stop the stats provider (optional)
    statsProvider.stop();


Expose the stats collected by the stats provider by configuring following settings:

::

    // enable exporting the stats
    statsExport=true
    // exporting the stats at port 8080
    statsHttpPort=8080


If exporting stats is enabled, all the stats are exported by the http endpoint.
You could curl the http endpoint to check the stats.

::

    curl -s <host>:8080/vars


check ScienceStats_ for more details.

.. _ScienceStats: https://github.com/twitter/commons/tree/master/src/java/com/twitter/common/stats

Twitter Ostrich Stats
_____________________

Use following dependency to enable Twitter ostrich stats provider.

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>twitter-ostrich-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>

Construct the stats provider for clients.

::

    StatsProvider statsProvider = new TwitterOstrichProvider();
    DistributedLogConfiguration conf = ...;

    // starts the stats provider (optional)
    statsProvider.start(conf);

    // all the dl related stats are exposed under "dlog"
    StatsLogger statsLogger = statsProvider.getStatsLogger("dlog");
    DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
        .uri(...)
        .conf(conf)
        .statsLogger(statsLogger)
        .build();

    ...

    // stop the stats provider (optional)
    statsProvider.stop();


Expose the stats collected by the stats provider by configuring following settings:

::

    // enable exporting the stats
    statsExport=true
    // exporting the stats at port 8080
    statsHttpPort=8080


If exporting stats is enabled, all the stats are exported by the http endpoint.
You could curl the http endpoint to check the stats.

::

    curl -s <host>:8080/stats.txt


check Ostrich_ for more details.

.. _Ostrich: https://github.com/twitter/ostrich

Twitter Finagle Metrics
_______________________

Use following dependency to enable bridging finagle stats receiver to bookkeeper's stats provider.
All the stats exposed by the stats provider will be collected by finagle stats receiver and exposed
by Twitter's admin service.

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>twitter-finagle-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>

Construct the stats provider for clients.

::

    StatsReceiver statsReceiver = ...; // finagle stats receiver
    StatsProvider statsProvider = new FinagleStatsProvider(statsReceiver);
    DistributedLogConfiguration conf = ...;

    // the stats provider does nothing on start.
    statsProvider.start(conf);

    // all the dl related stats are exposed under "dlog"
    StatsLogger statsLogger = statsProvider.getStatsLogger("dlog");
    DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
        .uri(...)
        .conf(conf)
        .statsLogger(statsLogger)
        .build();

    ...

    // the stats provider does nothing on stop.
    statsProvider.stop();


check `finagle metrics library`__ for more details on how to expose the stats.

.. _TwitterServer: https://twitter.github.io/twitter-server/Migration.html

__ TwitterServer_

Codahale Metrics
________________

Use following dependency to enable Twitter ostrich stats provider.

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>codahale-metrics-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>

Construct the stats provider for clients.

::

    StatsProvider statsProvider = new CodahaleMetricsProvider();
    DistributedLogConfiguration conf = ...;

    // starts the stats provider (optional)
    statsProvider.start(conf);

    // all the dl related stats are exposed under "dlog"
    StatsLogger statsLogger = statsProvider.getStatsLogger("dlog");
    DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
        .uri(...)
        .conf(conf)
        .statsLogger(statsLogger)
        .build();

    ...

    // stop the stats provider (optional)
    statsProvider.stop();


Expose the stats collected by the stats provider in different ways by configuring following settings.
Check Codehale_ on how to configuring report endpoints.

::

    // How frequent report the stats
    codahaleStatsOutputFrequencySeconds=...
    // The prefix string of codahale stats
    codahaleStatsPrefix=...

    //
    // Report Endpoints
    //

    // expose the stats to Graphite
    codahaleStatsGraphiteEndpoint=...
    // expose the stats to CSV files
    codahaleStatsCSVEndpoint=...
    // expose the stats to Slf4j logging
    codahaleStatsSlf4jEndpoint=...
    // expose the stats to JMX endpoint
    codahaleStatsJmxEndpoint=...


check Codehale_ for more details.

.. _Codehale: https://dropwizard.github.io/metrics/3.1.0/

Enable Stats Provider on Bookie Servers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The stats provider used by *Bookie Servers* is configured by setting the following option.

::

    // class of stats provider
    statsProviderClass="org.apache.bookkeeper.stats.CodahaleMetricsProvider"

Metrics
~~~~~~~

Check the :doc:`../references/metrics` reference page for the metrics exposed by DistributedLog.
