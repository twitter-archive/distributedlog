DistributedLog Operations
=========================

`dlog`
------

A CLI is provided for inspecting DistributedLog streams and metadata.

.. code:: bash

   dlog
   JMX enabled by default
   Usage: dlog <command>
   where command is one of:
       local               Run distributedlog sandbox
       example             Run distributedlog example
       tool                Run distributedlog tool
       proxy_tool          Run distributedlog proxy tool to interact with proxies
       balancer            Run distributedlog balancer
       admin               Run distributedlog admin tool
       help                This help message

   or command is the full name of a class with a defined main() method.

   Environment variables:
       DLOG_LOG_CONF        Log4j configuration file (default $HOME/src/distributedlog/distributedlog-service/conf/log4j.properties)
       DLOG_EXTRA_OPTS      Extra options to be passed to the jvm
       DLOG_EXTRA_CLASSPATH Add extra paths to the dlog classpath

These variable can also be set in conf/dlogenv.sh

Create a stream
~~~~~~~~~~~~~~~

To create a stream:

.. code:: bash

   dlog tool create -u <DL URI> -r <STREAM PREFIX> -e <STREAM EXPRESSION>


List the streams
~~~~~~~~~~~~~~~~

To list all the streams under a given DistributedLog namespace:

.. code:: bash

   dlog tool list -u <DL URI>

Show stream's information
~~~~~~~~~~~~~~~~~~~~~~~~~

To view the metadata associated with a stream:

.. code:: bash

   dlog tool show -u <DL URI> -s <STREAM NAME>


Dump a stream
~~~~~~~~~~~~~

To dump the items inside a stream:

.. code:: bash

   dlog dlog tool dump -u <DL URI> -s <STREAM NAME> -o <START TXN ID> -l <NUM RECORDS>

Delete a stream
~~~~~~~~~~~~~~~

To delete a stream, run:

.. code:: bash

   dlog tool delete -u <DL URI> -s <STREAM NAME>


Truncate a stream
~~~~~~~~~~~~~~~~~

Truncate the streams under a given DistributedLog namespace. You could specify a filter to match the streams that you want to truncate.

There is a difference between the ``truncate`` and ``delete`` command. When you issue a ``truncate``, the data will be purge without removing the streams. A ``delete`` will delete the stream. You can pass the flag ``-delete`` to the ``truncate`` command to also delete the streams.

.. code:: bash

   dlog tool truncate -u <DL URI>
