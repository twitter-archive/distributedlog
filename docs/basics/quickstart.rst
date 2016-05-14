Quick Start
===========

This tutorial assumes you are starting from fresh and have no existing BookKeeper or ZooKeeper data.

Step 1: Download the binary
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:doc:`Download <../download>` the stable version of `DistributedLog` and un-zip it.

::

    // Download the binary `distributedlog-all-${gitsha}.zip`
    > unzip distributedlog-all-${gitsha}.zip


Step 2: Start ZooKeeper & BookKeeper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DistributedLog uses `ZooKeeper` as the metadata store and `BookKeeper` as the log segment store. So
you need to first start a zookeeper server and a few bookies if you don't already have one. You can
use the `dlog` script in `distributedlog-service` package to get a standalone bookkeeper sandbox. It
starts a zookeeper server and `N` bookies (N is 3 by default).

::

    // Start the local sandbox instance at port `7000`
    > ./distributedlog-service/bin/dlog local 7000
    DistributedLog Sandbox is running now. You could access distributedlog://127.0.0.1:7000


Step 3: Create a DistributedLog namespace
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before using distributedlog, you need to create a distributedlog namespace to store your own list of
streams. The zkServer for the local sandbox is `127.0.0.1:7000` and the bookkeeper's ledgers path is
`/ledgers`. You could create a namespace pointing to the corresponding bookkeeper cluster.

::

    > ./distributedlog-service/bin/dlog admin bind -l /ledgers -s 127.0.0.1:7000 -c distributedlog://127.0.0.1:7000/messaging/my_namespace
    No bookkeeper is bound to distributedlog://127.0.0.1:7000/messaging/my_namespace
    Created binding on distributedlog://127.0.0.1:7000/messaging/my_namespace.


If you don't want to create a separated namespace, you could use the default namespace `distributedlog://127.0.0.1:7000/messaging/distributedlog`.


Step 4: Create some log streams
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's create 5 log streams, prefixed with `messaging-test-`.

::

    > ./distributedlog-service/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/my_namespace -r messaging-stream- -e 1-5


We can now see the streams if we run the `list` command from the tool.

::
    
    > ./distributedlog-service/bin/dlog tool list -u distributedlog://127.0.0.1:7000/messaging/my_namespace
    Streams under distributedlog://127.0.0.1:7000/messaging/my_namespace :
    --------------------------------
    messaging-stream-1
    messaging-stream-3
    messaging-stream-2
    messaging-stream-4
    messaging-stream-5
    --------------------------------


Step 5: Start a write proxy
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, lets start a write proxy server that serves writes to distributedlog namespace `distributedlog://127.0.0.1/messaging/my_namespace`. The server listens on 8000 to accept fan-in write requests.

::
    
    > ./distributedlog-service/bin/dlog-daemon.sh start writeproxy -p 8000 --shard-id 1 -sp 8001 -u distributedlog://127.0.0.1:7000/messaging/my_namespace -mx -c `pwd`/distributedlog-service/conf/distributedlog_proxy.conf


Step 6: Tail reading records
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The distributedlog tutorial has a multi-streams reader that will dump out received records to standard output.

::
    
    > ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.MultiReader distributedlog://127.0.0.1:7000/messaging/my_namespace messaging-stream-1,messaging-stream-2,messaging-stream-3,messaging-stream-4,messaging-stream-5


Step 7: Write some records
~~~~~~~~~~~~~~~~~~~~~~~~~~

The distributedlog tutorial also has a multi-streams writer that will take input from a console and write it out
as records to the distributedlog write proxy. Each line will be sent as a separate record.

Run the writer and type a few lines into the console to send to the server.

::
    
    > ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.ConsoleProxyMultiWriter 'inet!127.0.0.1:8000' messaging-stream-1,messaging-stream-2,messaging-stream-3,messaging-stream-4,messaging-stream-5

If you have each of the above commands running in a different terminal then you should now be able to type messages into the writer terminal and see them appear in the reader terminal.
