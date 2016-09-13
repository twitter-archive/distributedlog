---
layout: default

# Top navigation
top-nav-group: admin-guide
top-nav-pos: 5
top-nav-title: ZooKeeper

# Sub-level navigation
sub-nav-group: admin-guide
sub-nav-parent: admin-guide
sub-nav-id: zookeeper
sub-nav-pos: 5
sub-nav-title: ZooKeeper
---

ZooKeeper
=========

To run a DistributedLog ensemble, you'll need a set of Zookeeper
nodes. There is no constraints on the number of Zookeeper nodes you
need. One node is enough to run your cluster, but for reliability
purpose, you should run at least 3 nodes.

Version
-------

DistributedLog leverages zookeepr `multi` operations for metadata updates.
So the minimum version of zookeeper is 3.4.*. We recommend to run stable
zookeeper version `3.4.8`.

Run ZooKeeper from distributedlog source
----------------------------------------

Since `zookeeper` is one of the dependency of `distributedlog-service`. You could simply
run `zookeeper` servers using same set of scripts provided in `distributedlog-service`.
In the following sections, we will describe how to run zookeeper using the scripts provided
in `distributedlog-service`.

Build
+++++

First of all, build DistributedLog:

.. code-block:: bash

    $ mvn clean install -DskipTests

Configuration
+++++++++++++

The configuration file `zookeeper.conf.template` under `distributedlog-service/conf` is a template of
production configuration to run a zookeeper node. Most of the configuration settings are good for
production usage. You might need to configure following settings according to your environment and
hardware platform.

Ensemble
^^^^^^^^

You need to configure the zookeeper servers form this ensemble as below:

::
    
    server.1=127.0.0.1:2710:3710:participant;0.0.0.0:2181


Please check zookeeper_ website for more configurations.

Disks
^^^^^

You need to configure following settings according to the disk layout of your hardware.
It is recommended to put `dataLogDir` under a separated disk from others for performance.

::
    
    # the directory where the snapshot is stored.
    dataDir=/tmp/data/zookeeper
    
    # where txlog  are written
    dataLogDir=/tmp/data/zookeeper/txlog


Run
+++

As `zookeeper` is shipped as part of `distributedlog-service`, you could use the `dlog-daemon.sh`
script to start `zookeeper` as daemon thread.

Start the zookeeper:

.. code-block:: bash

    $ ./distributedlog-service/bin/dlog-daemon.sh start zookeeper /path/to/zookeeper.conf

Stop the zookeeper:

.. code-block:: bash

    $ ./distributedlog-service/bin/dlog-daemon.sh stop zookeeper

Please check zookeeper_ website for more details.

.. _zookeeper: http://zookeeper.apache.org/
