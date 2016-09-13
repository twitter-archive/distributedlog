---
layout: default

# Top navigation
top-nav-group: admin-guide
top-nav-pos: 6
top-nav-title: BookKeeper

# Sub-level navigation
sub-nav-group: admin-guide
sub-nav-parent: admin-guide
sub-nav-id: bookkeeper
sub-nav-pos: 6
sub-nav-title: BookKeeper
---

.. contents:: BookKeeper

BookKeeper
==========

For reliable BookKeeper service, you should deploy BookKeeper in a cluster.

Run from bookkeeper source
--------------------------

The version of BookKeeper that DistributedLog depends on is not the official opensource version.
It is twitter's production version `4.3.4-TWTTR`, which is available in `https://github.com/twitter/bookkeeper`. 
We are working actively with BookKeeper community to merge all twitter's changes back to the community.

The major changes in Twitter's bookkeeper includes:

- BOOKKEEPER-670_: Long poll reads and LastAddConfirmed piggyback. It is to reduce the tailing read latency.
- BOOKKEEPER-759_: Delay ensemble change if it doesn't break ack quorum constraint. It is to reduce the write latency on bookie failures.
- BOOKKEEPER-757_: Ledger recovery improvements, to reduce the latency on ledger recovery.
- Misc improvements on bookie recovery and bookie storage.

.. _BOOKKEEPER-670: https://issues.apache.org/jira/browse/BOOKKEEPER-670
.. _BOOKKEEPER-759: https://issues.apache.org/jira/browse/BOOKKEEPER-759
.. _BOOKKEEPER-757: https://issues.apache.org/jira/browse/BOOKKEEPER-757

To build bookkeeper, run:

1. First checkout the bookkeeper source code from twitter's branch.

.. code-block:: bash

    $ git clone https://github.com/twitter/bookkeeper.git bookkeeper   


2. Build the bookkeeper package:

.. code-block:: bash

    $ cd bookkeeper 
    $ mvn clean package assembly:single -DskipTests

However, since `bookkeeper-server` is one of the dependency of `distributedlog-service`.
You could simply run bookkeeper using same set of scripts provided in `distributedlog-service`.
In the following sections, we will describe how to run bookkeeper using the scripts provided in
`distributedlog-service`.

Run from distributedlog source
------------------------------

Build
+++++

First of all, build DistributedLog:

.. code-block:: bash

    $ mvn clean install -DskipTests


Configuration
+++++++++++++

The configuration file `bookie.conf` under `distributedlog-service/conf` is a template of production
configuration to run a bookie node. Most of the configuration settings are good for production usage.
You might need to configure following settings according to your environment and hardware platform.

Port
^^^^

By default, the service port is `3181`, where the bookie server listens on. You can change the port
to whatever port you like by modifying the following setting.

::

    bookiePort=3181


Disks
^^^^^

You need to configure following settings according to the disk layout of your hardware. It is recommended
to put `journalDirectory` under a separated disk from others for performance. It is okay to set
`indexDirectories` to be same as `ledgerDirectories`. However, it is recommended to put `indexDirectories`
to a SSD driver for better performance.

::
    
    # Directory Bookkeeper outputs its write ahead log
    journalDirectory=/tmp/data/bk/journal

    # Directory Bookkeeper outputs ledger snapshots
    ledgerDirectories=/tmp/data/bk/ledgers

    # Directory in which index files will be stored.
    indexDirectories=/tmp/data/bk/ledgers


To better understand how bookie nodes work, please check bookkeeper_ website for more details.

ZooKeeper
^^^^^^^^^

You need to configure following settings to point the bookie to the zookeeper server that it is using.
You need to make sure `zkLedgersRootPath` exists before starting the bookies.

::
   
    # Root zookeeper path to store ledger metadata
    # This parameter is used by zookeeper-based ledger manager as a root znode to
    # store all ledgers.
    zkLedgersRootPath=/messaging/bookkeeper/ledgers
    # A list of one of more servers on which zookeeper is running.
    zkServers=localhost:2181


Stats Provider
^^^^^^^^^^^^^^

Bookies use `StatsProvider` to expose its metrics. The `StatsProvider` is a pluggable library to
adopt to various stats collecting systems. Please check monitoring_ for more details.

.. _monitoring: ./monitoring

::
    
    # stats provide - use `codahale` metrics library
    statsProviderClass=org.apache.bookkeeper.stats.CodahaleMetricsServletProvider

    ### Following settings are stats provider related settings

    # Exporting codahale stats in http port `9001`
    codahaleStatsHttpPort=9001


Index Settings
^^^^^^^^^^^^^^

- `pageSize`: size of a index page in ledger cache, in bytes. If there are large number
  of ledgers and each ledger has fewer entries, smaller index page would improve memory usage.
- `pageLimit`: The maximum number of index pages in ledger cache. If nummber of index pages
  reaches the limitation, bookie server starts to swap some ledgers from memory to disk.
  Increase this value when swap becomes more frequent. But make sure `pageLimit*pageSize`
  should not be more than JVM max memory limitation.


Journal Settings
^^^^^^^^^^^^^^^^

- `journalMaxGroupWaitMSec`: The maximum wait time for group commit. It is valid only when
  `journalFlushWhenQueueEmpty` is false.
- `journalFlushWhenQueueEmpty`: Flag indicates whether to flush/sync journal. If it is `true`,
  bookie server will sync journal when there is no other writes in the journal queue.
- `journalBufferedWritesThreshold`: The maximum buffered writes for group commit, in bytes.
  It is valid only when `journalFlushWhenQueueEmpty` is false.
- `journalBufferedEntriesThreshold`: The maximum buffered writes for group commit, in entries.
  It is valid only when `journalFlushWhenQueueEmpty` is false.

Setting `journalFlushWhenQueueEmpty` to `true` will produce low latency when the traffic is low.
However, the latency varies a lost when the traffic is increased. So it is recommended to set
`journalMaxGroupWaitMSec`, `journalBufferedEntriesThreshold` and `journalBufferedWritesThreshold`
to reduce the number of fsyncs made to journal disk, to achieve sustained low latency.

Thread Settings
^^^^^^^^^^^^^^^

It is recommended to configure following settings to align with the cpu cores of the hardware.

::
    
    numAddWorkerThreads=4
    numJournalCallbackThreads=4
    numReadWorkerThreads=4
    numLongPollWorkerThreads=4

Run 
+++

As `bookkeeper-server` is shipped as part of `distributedlog-service`, you could use the `dlog-daemon.sh`
script to start `bookie` as daemon thread.

Start the bookie:

.. code-block:: bash

    $ ./distributedlog-service/bin/dlog-daemon.sh start bookie --conf /path/to/bookie/conf


Stop the bookie:

.. code-block:: bash

    $ ./distributedlog-service/bin/dlog-daemon.sh stop bookie


Please check bookkeeper_ website for more details.

.. _bookkeeper: http://bookkeeper.apache.org/
