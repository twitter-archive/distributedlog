Build
=====

You need to have maven and Java installed in order to build
DistributedLog.

Building a JAR
--------------

To build DistributedLog, run:

.. code-block:: bash

   mvn package

This will generate a zip file in
``distributedlog-service/target``. This zip file contains the main JAR
to run DistributedLog, and all the dependencies.

Running the tests
-----------------

.. code-block:: bash

   mvn test
