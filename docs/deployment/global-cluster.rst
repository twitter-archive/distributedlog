---
title: Global Cluster Setup

top-nav-group: deployment
top-nav-pos: 1
top-nav-title: Global Cluster Setup

# Sub-level navigation
sub-nav-group: admin-guide
sub-nav-id: cluster-deployment
sub-nav-parent: admin-guide
sub-nav-group-title: Global Cluster Setup
sub-nav-pos: 1
sub-nav-title: Global Cluster Setup

layout: default
---

.. contents:: This page provides instructions on how to run **DistributedLog** across multiple regions.


Cluster Setup & Deployment
==========================

Setting up `globally replicated DistributedLog <../user_guide/globalreplicatedlog/main>`_ is very similar to setting up local DistributedLog.
The most important change is use a ZooKeeper cluster configured across multiple-regions. Once set up, DistributedLog
and BookKeeper are configured to use the global ZK cluster for all metadata storage, and the system will more or
less work. The remaining steps are necessary to ensure things like durability in the face of total region failure.

The key differences with standard cluster setup are summarized below:

- The zookeeper cluster must be running across all of the target regions, say A, B, C.

- Region aware placement policy and a few other options must be configured in DL config.

- DistributedLog clients should be configured to talk to all regions.

We elaborate on these steps in the following sections.


Global Zookeeper
----------------

When defining your server and participant lists in zookeeper configuration, a sufficient number of nodes from each
region must be included.

Please consult the ZooKeeper documentation for detailed Zookeeper setup instructions.


DistributedLog Configuration
----------------------------

In multi-region DistributedLog several DL config changes are needed.

Placement Policy
++++++++++++++++

The region-aware placement policy must be configured. Below, it is configured to place replicas across 3 regions, A, B, and C.

::

    # placement policy
    bkc.ensemblePlacementPolicy=org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy
    bkc.reppRegionsToWrite=A;B;C
    bkc.reppMinimumRegionsForDurability=2
    bkc.reppEnableDurabilityEnforcementInReplace=true
    bkc.reppEnableValidation=true

Connection Timeouts
+++++++++++++++++++

In global replicated mode, the proxy nodes will be writing to the entire ensemble, which exists in multiple regions.
If cross-region latency is higher than local region latency (i.e. if its truly cross-region) then it is advisable to
use a higher BookKeeper client connection tiemout.

::

    # setting connect timeout to 1 second for global cluster
    bkc.connectTimeoutMillis=1000

Quorum Size
+++++++++++

It is advisable to run with a larger ensemble to ensure cluster health in the event of region loss (the ensemble
will be split across all regions).

The values of these settings will depend on your operational and durability requirements.

::

    ensemble-size=9
    write-quorum-size=9
    ack-quorum-size=5

Client Configuration
--------------------

Although not required, it is recommended to configure the write client to use all available regions. Several methods
in DistributedLogClientBuilder can be used to achieve this.

.. code-block:: java

    DistributedLogClientBuilder.serverSets
    DistributedLogClientBuilder.finagleNameStrs


Additional Steps
================

Other clients settings may need to be tuned - for example in the write client, timeouts will likely need to be
increased.

Aside from this however, cluster setup is exactly the same as `single region setup <cluster>`_.
