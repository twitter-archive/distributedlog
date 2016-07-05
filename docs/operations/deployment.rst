Cluster Setup & Deployment
==========================

This section describes how to run DistributedLog in `distributed` mode.
To run a cluster with DistributedLog, you need a Zookeeper cluster and a Bookkeeper cluster.

Build
-----

To build DistributedLog, run:

.. code-block:: bash

   mvn clean install -DskipTests


Or run `./scripts/snapshot` to build the release packages from current source. The released
packages contain the binaries for running `distributedlog-service`, `distributedlog-benchmark`
and `distributedlog-tutorials`.

NOTE: we run following instructions from distributedlog source code after running `mvn clean install`.
And assume `DL_HOME` is the directory of distributedlog source.

Zookeeper
---------

(If you already have a zookeeper cluster running, you could skip this section.)

We could use the `dlog-daemon.sh` and the `zookeeper.conf.template` to demonstrate run a 1-node
zookeeper ensemble locally.

Create a `zookeeper.conf` from the `zookeeper.conf.template`.

.. code-block:: bash

    $ cp distributedlog-service/conf/zookeeper.conf.template distributedlog-service/conf/zookeeper.conf

Configure the settings in `zookeeper.conf`. By default, it will use `/tmp/data/zookeeper` for storing
the zookeeper data. Let's create the data directories for zookeeper.

.. code-block:: bash

    $ mkdir -p /tmp/data/zookeeper/txlog

Once the data directory is created, we need to assign `myid` for this zookeeper node.

.. code-block:: bash

    $ echo "1" > /tmp/data/zookeeper/myid

Start the zookeeper daemon using `dlog-daemon.sh`.

.. code-block:: bash

    $ ./distributedlog-service/bin/dlog-daemon.sh start zookeeper ${DL_HOME}/distributedlog-service/conf/zookeeper.conf

You could verify the zookeeper setup using `zkshell`.

.. code-block:: bash

    // ./distributedlog-service/bin/dlog zkshell ${zkservers}
    $ ./distributedlog-service/bin/dlog zkshell localhost:2181
    Connecting to localhost:2181
    Welcome to ZooKeeper!
    JLine support is enabled

    WATCHER::

    WatchedEvent state:SyncConnected type:None path:null
    [zk: localhost:2181(CONNECTED) 0] ls /
    [zookeeper]
    [zk: localhost:2181(CONNECTED) 1]

Please refer to the :doc:`zookeeper` for more details on setting up zookeeper cluster.

Bookkeeper
----------

(If you already have a bookkeeper cluster running, you could skip this section.)

We could use the `dlog-daemon.sh` and the `bookie.conf.template` to demonstrate run a 3-nodes
bookkeeper cluster locally.

Create a `bookie.conf` from the `bookie.conf.template`. Since we are going to run a 3-nodes
bookkeeper cluster locally. Let's make three copies of `bookie.conf.template`.

.. code-block:: bash

    $ cp distributedlog-service/conf/bookie.conf.template distributedlog-service/conf/bookie-1.conf
    $ cp distributedlog-service/conf/bookie.conf.template distributedlog-service/conf/bookie-2.conf
    $ cp distributedlog-service/conf/bookie.conf.template distributedlog-service/conf/bookie-3.conf

Configure the settings in the bookie configuraiont files.

First of all, choose the zookeeper cluster that the bookies will use and set `zkServers` in
the configuration files.

::
    
    zkServers=localhost:2181

Choose the zookeeper path to store bookkeeper metadata and set `zkLedgersRootPath` in the configuration
files. Let's use `/messaging/bookkeeper/ledgers` in this instruction.

::

    zkLedgersRootPath=/messaging/bookkeeper/ledgers


Format bookkeeper metadata
++++++++++++++++++++++++++

(NOTE: only format bookkeeper metadata when first time setting up the bookkeeper cluster.)

The bookkeeper shell doesn't automatically create the `zkLedgersRootPath` when running `metaformat`.
So using `zkshell` to create the `zkLedgersRootPath`.

::

    $ ./distributedlog-service/bin/dlog zkshell localhost:2181
    Connecting to localhost:2181
    Welcome to ZooKeeper!
    JLine support is enabled

    WATCHER::

    WatchedEvent state:SyncConnected type:None path:null
    [zk: localhost:2181(CONNECTED) 0] create /messaging ''
    Created /messaging
    [zk: localhost:2181(CONNECTED) 1] create /messaging/bookkeeper ''
    Created /messaging/bookkeeper
    [zk: localhost:2181(CONNECTED) 2] create /messaging/bookkeeper/ledgers ''
    Created /messaging/bookkeeper/ledgers
    [zk: localhost:2181(CONNECTED) 3]


If the `zkLedgersRootPath`, run `metaformat` to format the bookkeeper metadata.

::
    
    $ BOOKIE_CONF=${DL_HOME}/distributedlog-service/conf/bookie-1.conf ./distributedlog-service/bin/dlog bkshell metaformat
    Are you sure to format bookkeeper metadata ? (Y or N) Y

Add Bookies
+++++++++++

Once the bookkeeper metadata is formatted, it is ready to add bookie nodes to the cluster.

Configure Ports
^^^^^^^^^^^^^^^

Configure the ports that used by bookies.

bookie-1:

::
   
    # Port that bookie server listen on
    bookiePort=3181
    # Exporting codahale stats
    185 codahaleStatsHttpPort=9001

bookie-2:

::
   
    # Port that bookie server listen on
    bookiePort=3182
    # Exporting codahale stats
    185 codahaleStatsHttpPort=9002

bookie-3:

::
   
    # Port that bookie server listen on
    bookiePort=3183
    # Exporting codahale stats
    185 codahaleStatsHttpPort=9003

Configure Disk Layout
^^^^^^^^^^^^^^^^^^^^^

Configure the disk directories used by a bookie server by setting following options.

::
    
    # Directory Bookkeeper outputs its write ahead log
    journalDirectory=/tmp/data/bk/journal
    # Directory Bookkeeper outputs ledger snapshots
    ledgerDirectories=/tmp/data/bk/ledgers
    # Directory in which index files will be stored.
    indexDirectories=/tmp/data/bk/ledgers

As we are configuring a 3-nodes bookkeeper cluster, we modify the following settings as below:

bookie-1:

::
    
    # Directory Bookkeeper outputs its write ahead log
    journalDirectory=/tmp/data/bk-1/journal
    # Directory Bookkeeper outputs ledger snapshots
    ledgerDirectories=/tmp/data/bk-1/ledgers
    # Directory in which index files will be stored.
    indexDirectories=/tmp/data/bk-1/ledgers

bookie-2:

::
    
    # Directory Bookkeeper outputs its write ahead log
    journalDirectory=/tmp/data/bk-2/journal
    # Directory Bookkeeper outputs ledger snapshots
    ledgerDirectories=/tmp/data/bk-2/ledgers
    # Directory in which index files will be stored.
    indexDirectories=/tmp/data/bk-2/ledgers

bookie-3:

::
    
    # Directory Bookkeeper outputs its write ahead log
    journalDirectory=/tmp/data/bk-3/journal
    # Directory Bookkeeper outputs ledger snapshots
    ledgerDirectories=/tmp/data/bk-3/ledgers
    # Directory in which index files will be stored.
    indexDirectories=/tmp/data/bk-3/ledgers

Format bookie
^^^^^^^^^^^^^

Once the disk directories are configured correctly in the configuration file, use
`bkshell bookieformat` to format the bookie.

::
    
    BOOKIE_CONF=${DL_HOME}/distributedlog-service/conf/bookie-1.conf ./distributedlog-service/bin/dlog bkshell bookieformat
    BOOKIE_CONF=${DL_HOME}/distributedlog-service/conf/bookie-2.conf ./distributedlog-service/bin/dlog bkshell bookieformat
    BOOKIE_CONF=${DL_HOME}/distributedlog-service/conf/bookie-3.conf ./distributedlog-service/bin/dlog bkshell bookieformat


Start bookie
^^^^^^^^^^^^

Start the bookie using `dlog-daemon.sh`.

::
    
    SERVICE_PORT=3181 ./distributedlog-service/bin/dlog-daemon.sh start bookie --conf ${DL_HOME}/distributedlog-service/conf/bookie-1.conf
    SERVICE_PORT=3182 ./distributedlog-service/bin/dlog-daemon.sh start bookie --conf ${DL_HOME}/distributedlog-service/conf/bookie-2.conf
    SERVICE_PORT=3183 ./distributedlog-service/bin/dlog-daemon.sh start bookie --conf ${DL_HOME}/distributedlog-service/conf/bookie-3.conf
    
Verify whether the bookie is setup correctly. You could simply check whether the bookie is showed up in
zookeeper `zkLedgersRootPath`/available znode.

::
    
    $ ./distributedlog-service/bin/dlog zkshell localhost:2181
    Connecting to localhost:2181
    Welcome to ZooKeeper!
    JLine support is enabled

    WATCHER::

    WatchedEvent state:SyncConnected type:None path:null
    [zk: localhost:2181(CONNECTED) 0] ls /messaging/bookkeeper/ledgers/available
    [127.0.0.1:3181, 127.0.0.1:3182, 127.0.0.1:3183, readonly]
    [zk: localhost:2181(CONNECTED) 1]


Or check if the bookie is exposing the stats at port `codahaleStatsHttpPort`.

::
    
    // ping the service
    $ curl localhost:9001/ping
    pong
    // checking the stats
    curl localhost:9001/metrics?pretty=true

Stop bookie
^^^^^^^^^^^

Stop the bookie using `dlog-daemon.sh`.

::
    
    $ ./distributedlog-service/bin/dlog-daemon.sh stop bookie
    // Example:
    $ SERVICE_PORT=3181 ./distributedlog-service/bin/dlog-daemon.sh stop bookie
    doing stop bookie ...
    stopping bookie
    Shutdown is in progress... Please wait...
    Shutdown completed.

Turn bookie to readonly
^^^^^^^^^^^^^^^^^^^^^^^

Start the bookie in `readonly` mode.

::
    
    $ SERVICE_PORT=3181 ./distributedlog-service/bin/dlog-daemon.sh start bookie --conf ${DL_HOME}/distributedlog-service/conf/bookie-1.conf --readonly

Verify if the bookie is running in `readonly` mode.

::
    
    $ ./distributedlog-service/bin/dlog zkshell localhost:2181
    Connecting to localhost:2181
    Welcome to ZooKeeper!
    JLine support is enabled

    WATCHER::

    WatchedEvent state:SyncConnected type:None path:null
    [zk: localhost:2181(CONNECTED) 0] ls /messaging/bookkeeper/ledgers/available
    [127.0.0.1:3182, 127.0.0.1:3183, readonly]
    [zk: localhost:2181(CONNECTED) 1] ls /messaging/bookkeeper/ledgers/available/readonly
    [127.0.0.1:3181]
    [zk: localhost:2181(CONNECTED) 2]

Please refer to the :doc:`bookkeeper` for more details on setting up bookkeeper cluster.

Create Namespace
----------------

After setting up a zookeeper cluster and a bookkeeper cluster, you could provision DL namespaces
for applications to use.

Provisioning a DistributedLog namespace is accomplished via the `bind` command available in `dlog tool`.

Namespace is bound by writing bookkeeper environment settings (e.g. the ledger path, bkLedgersZkPath,
or the set of Zookeeper servers used by bookkeeper, bkZkServers) as metadata in the zookeeper path of
the namespace DL URI. The DL library resolves the DL URI to determine which bookkeeper cluster it
should read and write to. 

The namespace binding has following features:

- `Inheritance`: suppose `distributedlog://<zkservers>/messaging/distributedlog` is bound to bookkeeper
  cluster `X`. All the streams created under `distributedlog://<zkservers>/messaging/distributedlog`,
  will write to bookkeeper cluster `X`.
- `Override`: suppose `distributedlog://<zkservers>/messaging/distributedlog` is bound to bookkeeper
  cluster `X`. You want streams under `distributedlog://<zkservers>/messaging/distributedlog/S` write
  to bookkeeper cluster `Y`. You could just bind `distributedlog://<zkservers>/messaging/distributedlog/S`
  to bookkeeper cluster `Y`. The binding to `distributedlog://<zkservers>/messaging/distributedlog/S`
  only affects streams under `distributedlog://<zkservers>/messaging/distributedlog/S`.

Create namespace binding using `dlog tool`. For example, we create a namespace
`distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace` pointing to the
bookkeeper cluster we just created above.

::
    
    $ distributedlog-service/bin/dlog admin bind \\
        -dlzr 127.0.0.1:2181 \\
        -dlzw 127.0.0.1:2181 \\
        -s 127.0.0.1:2181 \\
        -bkzr 127.0.0.1:2181 \\
        -l /messaging/bookkeeper/ledgers \\
        -i false \\
        -r true \\
        -c \\
        distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace

    No bookkeeper is bound to distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace
    Created binding on distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace.


- Configure the zookeeper cluster used for storing DistributedLog metadata: `-dlzr` and `-dlzw`.
  Ideally `-dlzr` and `-dlzw` would be same the zookeeper server in distributedlog namespace uri.
  However to scale zookeeper reads, the zookeeper observers sometimes are added in a different
  domain name than participants. In such case, configuring `-dlzr` and `-dlzw` to different
  zookeeper domain names would help isolating zookeeper write and read traffic.
- Configure the zookeeper cluster used by bookkeeper for storing the metadata : `-bkzr` and `-s`.
  Similar as `-dlzr` and `-dlzw`, you could configure the namespace to use different zookeeper
  domain names for readers and writers to access bookkeeper metadatadata.
- Configure the bookkeeper ledgers path: `-l`.
- Configure the zookeeper path to store DistributedLog metadata. It is implicitly included as part
  of namespace URI.

Write Proxy
-----------

A write proxy consists of multiple write proxies. They don't store any state locally. So they are
mostly stateless and can be run as many as you can.

Configuration
+++++++++++++

Different from bookkeeper, DistributedLog tries not to configure any environment related settings
in configuration files. Any environment related settings are stored and configured via `namespace binding`.
The configuration file should contain non-environment related settings.

There is a `write_proxy.conf` template file available under `distributedlog-service` module.

Run write proxy
+++++++++++++++

A write proxy could be started using `dlog-daemon.sh` script under `distributedlog-service`.

::
    
    WP_SHARD_ID=${WP_SHARD_ID} WP_SERVICE_PORT=${WP_SERVICE_PORT} WP_STATS_PORT=${WP_STATS_PORT} ./distributedlog-service/bin/dlog-daemon.sh start writeproxy

- `WP_SHARD_ID`: A non-negative integer. You don't need to guarantee uniqueness of shard id, as it is just an
  indicator to the client for routing the requests. If you are running the `write proxy` using a cluster scheduler
  like `aurora`, you could easily obtain a shard id and use that to configure `WP_SHARD_ID`.
- `WP_SERVICE_PORT`: The port that write proxy listens on.
- `WP_STATS_PORT`: The port that write proxy exposes stats to a http endpoint.

Please check `distributedlog-service/conf/dlogenv.sh` for more environment variables on configuring write proxy.

- `WP_CONF_FILE`: The path to the write proxy configuration file.
- `WP_NAMESPACE`: The distributedlog namespace that the write proxy is serving for.

For example, we start 3 write proxies locally and point to the namespace created above.

::
    
    $ WP_SHARD_ID=1 WP_SERVICE_PORT=4181 WP_STATS_PORT=20001 ./distributedlog-service/bin/dlog-daemon.sh start writeproxy
    $ WP_SHARD_ID=2 WP_SERVICE_PORT=4182 WP_STATS_PORT=20002 ./distributedlog-service/bin/dlog-daemon.sh start writeproxy
    $ WP_SHARD_ID=3 WP_SERVICE_PORT=4183 WP_STATS_PORT=20003 ./distributedlog-service/bin/dlog-daemon.sh start writeproxy

The write proxy will announce itself to the zookeeper path `.write_proxy` under the dl namespace path.

We could verify that the write proxy is running correctly by checking the zookeeper path or checking its stats port.

::
    $ ./distributedlog-service/bin/dlog zkshell localhost:2181
    Connecting to localhost:2181
    Welcome to ZooKeeper!
    JLine support is enabled

    WATCHER::

    WatchedEvent state:SyncConnected type:None path:null
    [zk: localhost:2181(CONNECTED) 0] ls /messaging/distributedlog/mynamespace/.write_proxy
    [member_0000000000, member_0000000001, member_0000000002]


::
    
    $ curl localhost:20001/ping
    pong


Add and Remove Write Proxies
++++++++++++++++++++++++++++

Removing a write proxy is pretty straightforward by just killing the process.

::
    
    WP_SHARD_ID=1 WP_SERVICE_PORT=4181 WP_STATS_PORT=10001 ./distributedlog-service/bin/dlog-daemon.sh stop writeproxy


Adding a new write proxy is just adding a new host and starting the write proxy
process as described above.

Write Proxy Naming
++++++++++++++++++

The `dlog-daemon.sh` script starts the write proxy by announcing it to the `.write_proxy` path under
the dl namespace. So you could use `zk!<zkservers>!/<namespace_path>/.write_proxy` as the finagle name
to access the write proxy cluster. It is `zk!127.0.0.1:2181!/messaging/distributedlog/mynamespace/.write_proxy`
in the above example.

Verify the setup
++++++++++++++++

You could verify the write proxy cluster by running tutorials over the setup cluster.

Create 10 streams.

::
    
    $ ./distributedlog-service/bin/dlog tool create -u distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace -r stream- -e 0-10
    You are going to create streams : [stream-0, stream-1, stream-2, stream-3, stream-4, stream-5, stream-6, stream-7, stream-8, stream-9, stream-10] (Y or N) Y


Tail read from the 10 streams.

::
    
    $ ./distributedlog-tutorials/distributedlog-basic/bin/runner run c.twitter.distributedlog.basic.MultiReader distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace stream-0,stream-1,stream-2,stream-3,stream-4,stream-5,stream-6,stream-7,stream-8,stream-9,stream-10


Run record generator over some streams

::
    
    $ ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.RecordGenerator 'zk!127.0.0.1:2181!/messaging/distributedlog/mynamespace/.write_proxy' stream-0 100
    $ ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.RecordGenerator 'zk!127.0.0.1:2181!/messaging/distributedlog/mynamespace/.write_proxy' stream-1 100


Check the terminal running `MultiReader`. You will see similar output as below:

::
    
    """
    Received record DLSN{logSegmentSequenceNo=1, entryId=21044, slotId=0} from stream stream-0
    """
    record-1464085079105
    """
    Received record DLSN{logSegmentSequenceNo=1, entryId=21046, slotId=0} from stream stream-0
    """
    record-1464085079113
    """
    Received record DLSN{logSegmentSequenceNo=1, entryId=9636, slotId=0} from stream stream-1
    """
    record-1464085079110
    """
    Received record DLSN{logSegmentSequenceNo=1, entryId=21048, slotId=0} from stream stream-0
    """
    record-1464085079125
    """
    Received record DLSN{logSegmentSequenceNo=1, entryId=9638, slotId=0} from stream stream-1
    """
    record-1464085079121
    """
    Received record DLSN{logSegmentSequenceNo=1, entryId=21050, slotId=0} from stream stream-0
    """
    record-1464085079133
    """
    Received record DLSN{logSegmentSequenceNo=1, entryId=9640, slotId=0} from stream stream-1
    """
    record-1464085079130
    """



Please refer to the :doc:`performance` for more details on tuning performance.
