---
layout: default

# Top navigation
top-nav-group: admin-guide
top-nav-pos: 2
top-nav-title: Load Test

# Sub-level navigation
sub-nav-group: admin-guide
sub-nav-parent: admin-guide
sub-nav-id: performance
sub-nav-pos: 2
sub-nav-title: Load Test
---

.. contents:: Load Test

Load Test
=========

Overview
--------

Under distributedlog-benchmark you will find a set of applications intended for generating large amounts of load in a distributedlog custer. These applications are suitable for load testing, performance testing, benchmarking, or even simply smoke testing a distributedlog cluster.

The dbench script can run in several modes:

1. bkwrite - Benchmark the distributedlog write path using the core library

2. write - Benchmark the distributedlog write path, via write proxy, using the thin client

3. read - Benchmark the distributedlog read path using the core library


Running Dbench
--------------

The distributedlog-benchmark binary dbench is intended to be run simultaneously from many machines with identical settings. Together, all instances of dbench comprise a benchmark job. How you launch a benchmark job will depend on your operating environment. We recommend using a cluster scheduler like aurora or kubernetes to simplify the process, but tools like capistrano can also simplify this process greatly.

The benchmark script can be found at

::

    distributedlog-benchmark/bin/dbench

Arguments may be passed to this script via environment variables. The available arguments depend on the execution mode. For an up to date list, check the script itself.


Write to Proxy with Thin Client
-------------------------------

The proxy write test (mode = `write`) can be used to send writes to a proxy cluster to be written to a set of streams.

For example to use the proxy write test to generate 10000 requests per second across 10 streams using 50 machines, run the following command on each machine.

::

    STREAM_NAME_PREFIX=loadtest_
    BENCHMARK_DURATION=3600 # seconds
    DL_NAMESPACE=<dl namespace>
    NUM_STREAMS=10
    INITIAL_RATE=200
    distributedlog-benchmark/bin/dbench write


Write to BookKeeper with Core Library
-------------------------------------

The core library write test (mode = `bkwrite`) can be used to send writes to directly to bookkeeper using the core library.

For example to use the core library write test to generate 100MBps across 10 streams using 100 machines, run the following command on each machine.

::

    STREAM_NAME_PREFIX=loadtest_
    BENCHMARK_DURATION=3600 # seconds
    DL_NAMESPACE=<dl namespace>
    NUM_STREAMS=10
    INITIAL_RATE=1024
    MSG_SIZE=1024
    distributedlog-benchmark/bin/dbench bkwrite


Read from BookKeeper with Core Library
--------------------------------------

The core library read test (mode = `read`) can be used to read directly from bookkeeper using the core library.

For example to use the core library read test to read from 10 streams on 100 instances, run the following command on each machine.

::

    STREAM_NAME_PREFIX=loadtest_
    BENCHMARK_DURATION=3600 # seconds
    DL_NAMESPACE=<dl namespace>
    MAX_STREAM_ID=9
    NUM_READERS_PER_STREAM=5
    TRUNCATION_INTERVAL=60 # seconds
    distributedlog-benchmark/bin/dbench read
