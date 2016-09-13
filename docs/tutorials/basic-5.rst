---
title: API - Read Records
top-nav-group: quickstart
top-nav-pos: 4
top-nav-title: API - Read Records
layout: default
---

.. contents:: Basic Tutorial - Tail reading records from a stream

Tail reading records from a stream
==================================

This tutorial shows how to tail read records from a stream.

.. sectnum::

Open a distributedlog manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create distributedlog URI
-------------------------

::

        String dlUriStr = ...;
        URI uri = URI.create(dlUriStr);


Create distributedlog configuration
-----------------------------------

::

        DistributedLogConfiguration conf = new DistributedLogConfiguration();


Build the distributedlog namespace
----------------------------------

::

        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build(); 


Open the distributedlog manager
-------------------------------

::

        DistributedLogManager dlm = namespace.openLog("basic-stream-9");


Get Last Record
~~~~~~~~~~~~~~~

Get the last record from the record. From the last record, we can use `DLSN` of last record
to start tailing the stream.

::

        LogRecordWithDLSN record = dlm.getLastLogRecord();
        DLSN lastDLSN = record.getDlsn();


Read Records
~~~~~~~~~~~~

Open the stream
---------------

Open the stream to start read the records.

::

        AsyncLogReader reader = FutureUtils.result(dlm.openAsyncLogReader(lastDLSN));


Read the records
----------------

Read the next available record from the stream. The future is satisified when the record is available.

::

        Future<LogRecordWithDLSN> readFuture = reader.readNext();


Register the read callback
---------------------------

Register a future listener on read completion. The reader will be notified once the record is ready for consuming.

::

        final FutureEventListener<LogRecordWithDLSN> readListener = new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                // executed when read failed.
            }

            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                // process the record
                ...
                // issue read next
                reader.readNext().addEventListener(this);
            }
        };
        reader.readNext().addEventListener(readListener);


Close the reader
~~~~~~~~~~~~~~~~

Close the reader after usage.

::

        FutureUtils.result(reader.asyncClose());


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

        // Create Stream `basic-stream-9`
        // dlog tool create -u ${distributedlog-uri} -r ${stream-prefix} -e ${stream-regex}
        ./distributedlog-core/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/distributedlog -r basic-stream- -e 9


Tail the stream
---------------

Tailing the stream using `TailReader` to wait for new records.

::

        // Tailing Stream `basic-stream-9`
        // runner run com.twitter.distributedlog.basic.TailReader ${distributedlog-uri} ${stream}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.TailReader distributedlog://127.0.0.1:7000/messaging/distributedlog basic-stream-9


Write records
-------------

Run the example to write records to the stream in a console.

::

        // Write Records into Stream `basic-stream-9`
        // runner run com.twitter.distributedlog.basic.ConsoleProxyWriter ${distributedlog-uri} ${stream}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.ConsoleProxyWriter 'inet!127.0.0.1:8000' basic-stream-9


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
        Opening log stream basic-stream-9
        Log stream basic-stream-9 is empty.
        Wait for records starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        """
        test-proxy-writer
        """
        

