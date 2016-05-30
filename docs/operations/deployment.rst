Cluster Setup & Deployment
==========================

To run a cluster with DistributedLog, you need a Zookeeper cluster and
a Bookkeeper cluster.

Apache Zookeeper
----------------

To run a DistributedLog ensemble, you'll need a set of Zookeeper
nodes. There is no constraints on the number of Zookeeper nodes you
need. One node is enough to run your cluster, but for reliability
purpose, you should run at least 3 nodes in production.

Since DistributedLog persist some meta-data in zookeeper, you'll need
to make sure your data are stored on persistent disks.

Apache Bookkeeper
-----------------

In a normal setup, you'll want at least a number of bookies equal to
your replication factor plus one. If you choose to have 3 replicas,
you'll want 4 bookies, that way if one of them goes down, you still
have enough hosts for the quorum.

The bookies are the storage layer in DistributedLog. They should run
on hosts with persistent storage.

Metadata
~~~~~~~~

Some metadata need to be created in Zookeeper before starting the
bookies. Get a copy of the bookkeeper binary and run the following
command to create them:

.. code-block:: bash

   bookkeeper-server/bin/bookkeeper shell metaformat [-nonInteractive] [-force]

Binding
~~~~~~~

After provisioning a Bookkeeper cluster (or if you plan to use an
existing cluster) you'll need to setup your DL cluster
environment. There are two parts to this process:

- Provision DL namespace in Zookeeper for your cluster (identified by
  a "distributedlog://" uri).
- Setup a DL write proxy cluster and point it to your DL uri (see
  proxy deploy instructions below).

Provisioning a DL namespace is accomplished via the "bind" command
available in the dlog tool.

Bind works by writing bookkeeper environment settings (e.g. the ledger
path, bkLedgersZkPath, or the set of Zookeeper servers used by
bookkeeper, bkZkServers) as metadata at some DL URI. The DL library
resolves the DL URI to determine which bookkeeper cluster it should
read and write to.

A DLOG client thus connects to a specific Bookkeeper/DL cluster solely
via a configured DL URI.

Features of binding:

- Inheritance : suppose
  distributedlog://<zkservers>/messaging/distributedlog is bound to
  bookkeeper cluster X. All the streams created under
  distributedlog://<zkservers>/messaging/distributedlog, will write to
  bookkeeper cluster X.
- Override: suppose
  distributedlog://<zkservers>/messaging/distributedlog is bound to
  bookkeeper cluster X. You want stream S
  distributedlog://<zkservers>/messaging/distributedlog/S write to
  bookkeeper cluster Y. You could just bind
  distributedlog://<zkservers>/messaging/distributedlog/S to
  bookkeeper cluster Y. The binding to
  distributedlog://<zkservers>/messaging/distributedlog/S only affects
  streams under
  distributedlog://<zkservers>/messaging/distributedlog/S.

Bind is managed when provisioning streams by administrator through
dlog tool.

.. code-block:: bash

   bin/dlog admin bind -c -i false -l /ledgers -r true -s <zkservers> distributedlog://<zkservers>/messaging/distributedlog


Write Proxies
-------------

