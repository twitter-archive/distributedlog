---
title: API - Write Records (via write proxy)
top-nav-group: quickstart
top-nav-pos: 3
top-nav-title: API - Write Records (via write proxy)
layout: default
---

.. contents:: Basic Tutorial - Using Proxy Client to write records

Basic Tutorial - Write Records using Write Proxy Client
=======================================================

This tutorial shows how to write records using write proxy client.

.. sectnum::

Open a write proxy client
~~~~~~~~~~~~~~~~~~~~~~~~~

Create write proxy client builder
---------------------------------

::

        DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder();
                .clientId(ClientId.apply("console-proxy-writer"))
                .name("console-proxy-writer");


Enable thrift mux
-----------------

::

        builder = builder.thriftmux(true);


Point the client to write proxy using finagle name
--------------------------------------------------

::

        String finagleNameStr = "inet!127.0.0.1:8000";
        builder = builder.finagleNameStr(finagleNameStr);


Build the write proxy client
----------------------------

::

        DistributedLogClient client = builder.build();


Write Records
~~~~~~~~~~~~~

Write records to a stream 
-------------------------

Application does not have to provide `TransactionID` on writing.
The `TransactionID` of a record is assigned by the write proxy.

::

        String streamName = "basic-stream-2";
        byte[] data = ...;
        Future<DLSN> writeFuture = client.write(streamName, ByteBuffer.wrap(data));


Register the write callback
---------------------------

Register a future listener on write completion. The writer will be notified once the write is completed.

::

        writeFuture.addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                // executed when write failed.
            }

            @Override
            public void onSuccess(DLSN value) {
                // executed when write completed.
            }
        });


Close the write proxy client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Close the write proxy client after usage.

::

        client.close();


Run the tutorial
~~~~~~~~~~~~~~~~

Run the example in the following steps:

Start the local bookkeeper cluster
----------------------------------

You can use follow command to start the distributedlog stack locally.
After the distributedlog cluster is started, you could access it using
distributedlog uri *distributedlog://127.0.0.1:7000/messaging/distributedlog*.

::

        // dlog local ${zk-port}
        ./distributedlog-core/bin/dlog local 7000


Start the write proxy
---------------------

Start the write proxy, listening on port 8000.

::

        // DistributedLogServerApp -p ${service-port} --shard-id ${shard-id} -sp ${stats-port} -u {distributedlog-uri} -mx -c ${conf-file}
        ./distributedlog-service/bin/dlog com.twitter.distributedlog.service.DistributedLogServerApp -p 8000 --shard-id 1 -sp 8001 -u distributedlog://127.0.0.1:7000/messaging/distributedlog -mx -c ${distributedlog-repo}/distributedlog-service/conf/distributedlog_proxy.conf


Create the stream
-----------------

Create the stream under the distributedlog uri.

::

        // Create Stream `basic-stream-2`
        // dlog tool create -u ${distributedlog-uri} -r ${stream-prefix} -e ${stream-regex}
        ./distributedlog-core/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/distributedlog -r basic-stream- -e 2


Tail the stream
---------------

Tailing the stream using `TailReader` to wait for new records.

::

        // Tailing Stream `basic-stream-2`
        // runner run com.twitter.distributedlog.basic.TailReader ${distributedlog-uri} ${stream}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.TailReader distributedlog://127.0.0.1:7000/messaging/distributedlog basic-stream-2


Write records
-------------

Run the example to write records to the stream in a console.

::

        // Write Records into Stream `basic-stream-2`
        // runner run com.twitter.distributedlog.basic.ConsoleProxyWriter ${distributedlog-uri} ${stream}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.ConsoleProxyWriter 'inet!127.0.0.1:8000' basic-stream-2


Check the results
-----------------

Example output from `ConsoleProxyWriter` and `TailReader`.

::

        // Output of `ConsoleProxyWriter`
        May 08, 2016 10:27:41 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[inet] = com.twitter.finagle.InetResolver(com.twitter.finagle.InetResolver@756d7bba)
        May 08, 2016 10:27:41 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fixedinet] = com.twitter.finagle.FixedInetResolver(com.twitter.finagle.FixedInetResolver@1d2e91f5)
        May 08, 2016 10:27:41 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[neg] = com.twitter.finagle.NegResolver$(com.twitter.finagle.NegResolver$@5c707aca)
        May 08, 2016 10:27:41 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[nil] = com.twitter.finagle.NilResolver$(com.twitter.finagle.NilResolver$@5c8d932f)
        May 08, 2016 10:27:41 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fail] = com.twitter.finagle.FailResolver$(com.twitter.finagle.FailResolver$@52ba2221)
        May 08, 2016 10:27:41 AM com.twitter.finagle.Init$$anonfun$1 apply$mcV$sp
        [dlog] > test-proxy-writer
        [dlog] >


        // Output of `TailReader`
        Opening log stream basic-stream-2
        Log stream basic-stream-2 is empty.
        Wait for records starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        """
        test-proxy-writer
        """


Attempt a second writer
-----------------------

Open another terminal to run `ConsoleProxyWriter`. The write should succeed as write proxy is able to accept
fan-in writes. Please checkout section `Considerations` to see the difference between **Write Ordering** and
**Read Ordering**.

::

         May 08, 2016 10:31:54 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
         INFO: Resolver[inet] = com.twitter.finagle.InetResolver(com.twitter.finagle.InetResolver@756d7bba)
         May 08, 2016 10:31:54 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
         INFO: Resolver[fixedinet] = com.twitter.finagle.FixedInetResolver(com.twitter.finagle.FixedInetResolver@1d2e91f5)
         May 08, 2016 10:31:54 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
         INFO: Resolver[neg] = com.twitter.finagle.NegResolver$(com.twitter.finagle.NegResolver$@5c707aca)
         May 08, 2016 10:31:54 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
         INFO: Resolver[nil] = com.twitter.finagle.NilResolver$(com.twitter.finagle.NilResolver$@5c8d932f)
         May 08, 2016 10:31:54 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
         INFO: Resolver[fail] = com.twitter.finagle.FailResolver$(com.twitter.finagle.FailResolver$@52ba2221)
         May 08, 2016 10:31:54 AM com.twitter.finagle.Init$$anonfun$1 apply$mcV$sp
         [dlog] > test-write-proxy-message-2
         [dlog] >
