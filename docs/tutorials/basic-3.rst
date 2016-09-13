---
title: API - Write Records to Multiple Streams
layout: default
---

.. contents:: Basic Tutorial - Write Records to Multiple Streams

Write Records to Multiple Streams
=================================

This tutorial shows how to write records using write proxy multi stream writer. The `DistributedLogMultiStreamWriter`
is a wrapper over `DistributedLogClient` on writing records to a set of streams in a `round-robin` way and ensure low write latency even on single stream ownership failover.

.. sectnum::

Open a write proxy client
~~~~~~~~~~~~~~~~~~~~~~~~~

Before everything, you have to open a write proxy client to write records.
These are the steps to follow to `open a write proxy client`.

Create write proxy client builder
---------------------------------

::

        DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder()
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


Create a `MultiStreamWriter`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create multi stream writer builder
----------------------------------

::

        DistributedLogMultiStreamWriterBuilder builder = DistributedLogMultiStreamWriter.newBuilder();


Build the writer to write a set of streams
------------------------------------------

::

        List<String> streams = ...;
        builder = builder.streams(streams);


Point the multi stream writer to use write proxy client
-------------------------------------------------------

::

        builder = builder.client(client);


Configure the flush policy for the multi stream writer
------------------------------------------------------

::

        // transmit immediately after a record is written.
        builder = builder.bufferSize(0);
        builder = builder.flushIntervalMs(0);


Configure the request timeouts and retry policy for the multi stream writer
---------------------------------------------------------------------------

::

        // Configure the speculative timeouts - if writing to a stream cannot
        // complete within the speculative timeout, it would try writing to
        // another streams.
        builder = builder.firstSpeculativeTimeoutMs(10000)
        builder = builder.maxSpeculativeTimeoutMs(20000)
        // Configure the request timeout.
        builder = builder.requestTimeoutMs(50000);


Build the multi writer
----------------------

::

        DistributedLogMultiStreamWriter writer = builder.build();


Write Records
~~~~~~~~~~~~~

Write records to multi streams 
------------------------------

::

        byte[] data = ...;
        Future<DLSN> writeFuture = writer.write(ByteBuffer.wrap(data));


Register the write callback
---------------------------

Register a future listener on write completion.

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


Run the tutorial
~~~~~~~~~~~~~~~~

Run the example in the following steps:

Start the local bookkeeper cluster
----------------------------------

You can use follow command to start the distributedlog stack locally.
After the distributedlog is started, you could access it using
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


Create multiple streams
-----------------------

Create multiple streams under the distributedlog uri.

::

        // Create Stream `basic-stream-{3-7}`
        // dlog tool create -u ${distributedlog-uri} -r ${stream-prefix} -e ${stream-regex}
        ./distributedlog-core/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/distributedlog -r basic-stream- -e 3-7


Tail the streams
----------------

Tailing the streams using `MultiReader` to wait for new records.

::

        // Tailing Stream `basic-stream-{3-7}`
        // runner run com.twitter.distributedlog.basic.MultiReader ${distributedlog-uri} ${stream}[,${stream}]
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.MultiReader distributedlog://127.0.0.1:7000/messaging/distributedlog basic-stream-3,basic-stream-4,basic-stream-5,basic-stream-6,basic-stream-7


Write the records
-----------------

Run the example to write records to the multi streams in a console.

::

        // Write Records into Stream `basic-stream-{3-7}`
        // runner run com.twitter.distributedlog.basic.ConsoleProxyMultiWriter ${distributedlog-uri} ${stream}[,${stream}]
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.ConsoleProxyMultiWriter 'inet!127.0.0.1:8000' basic-stream-3,basic-stream-4,basic-stream-5,basic-stream-6,basic-stream-7

Check the results
-----------------

Example output from `ConsoleProxyMultiWriter` and `MultiReader`.

::

        // Output of `ConsoleProxyWriter`
        May 08, 2016 11:09:21 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[inet] = com.twitter.finagle.InetResolver(com.twitter.finagle.InetResolver@fbb628c)
        May 08, 2016 11:09:21 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fixedinet] = com.twitter.finagle.FixedInetResolver(com.twitter.finagle.FixedInetResolver@5a25adb1)
        May 08, 2016 11:09:21 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[neg] = com.twitter.finagle.NegResolver$(com.twitter.finagle.NegResolver$@5fae6db3)
        May 08, 2016 11:09:21 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[nil] = com.twitter.finagle.NilResolver$(com.twitter.finagle.NilResolver$@34a433d8)
        May 08, 2016 11:09:21 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fail] = com.twitter.finagle.FailResolver$(com.twitter.finagle.FailResolver$@847c4e8)
        May 08, 2016 11:09:22 AM com.twitter.finagle.Init$$anonfun$1 apply$mcV$sp
        [dlog] > message-1
        [dlog] > message-2
        [dlog] > message-3
        [dlog] > message-4
        [dlog] > message-5
        [dlog] >


        // Output of `MultiReader`
        Opening log stream basic-stream-3
        Opening log stream basic-stream-4
        Opening log stream basic-stream-5
        Opening log stream basic-stream-6
        Opening log stream basic-stream-7
        Log stream basic-stream-4 is empty.
        Wait for records from basic-stream-4 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream basic-stream-4
        Log stream basic-stream-5 is empty.
        Wait for records from basic-stream-5 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream basic-stream-5
        Log stream basic-stream-6 is empty.
        Wait for records from basic-stream-6 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream basic-stream-6
        Log stream basic-stream-3 is empty.
        Wait for records from basic-stream-3 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream basic-stream-3
        Log stream basic-stream-7 is empty.
        Wait for records from basic-stream-7 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream basic-stream-7
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0} from stream basic-stream-4
        """
        message-1
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0} from stream basic-stream-6
        """
        message-2
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0} from stream basic-stream-3
        """
        message-3
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0} from stream basic-stream-7
        """
        message-4
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0} from stream basic-stream-5
        """
        message-5
        """
