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
+++++++++++++++++++++

Use following 

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>twitter-science-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>


check `Science Stats`__ for more details.

.. _ScienceStats: https://github.com/twitter/commons/tree/master/src/java/com/twitter/common/stats

__ ScienceStats_


* *Twitter Ostrich*: check Ostrich_ for more details.

.. _Ostrich: https://github.com/twitter/ostrich

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>twitter-ostrich-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>


* *Twitter Finagle Metrics*: check `finagle metrics library` for more details.

.. _TwitterServer: https://twitter.github.io/twitter-server/Migration.html

__ TwitterServer_

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>twitter-finagle-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>


* *Codahale Metrics*: check Codehale_ for more details.

.. _Codehale: https://dropwizard.github.io/metrics/3.1.0/

::

   <dependency>
     <groupId>org.apache.bookkeeper.stats</groupId>
     <artifactId>codahale-metrics-provider</artifactId>
     <version>${bookkeeper.version}</version>
   </dependency>


