Cluster Setup & Deployment
==========================

To run a cluster with DistributedLog, you need a Zookeeper cluster and a Bookkeeper cluster.

Build
-----

To build DistributedLog, run:

.. code-block:: bash

   mvn package

This will generate a zip file in ``distributedlog-service/target``. This zip file contains the main JAR to run DistributedLog, and all the dependencies.

Zookeeper
---------

Please refer to the :doc:`/zookeeper` configuration.

Bookkeeper
----------

There is no minimal instances needed to run a Bookkeeper cluster.

DistributedLog
--------------
