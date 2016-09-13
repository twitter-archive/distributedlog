---
title: API - Write Records (via core library)
top-nav-group: quickstart
top-nav-pos: 2
top-nav-title: API - Write Records (via core library)
layout: default
---

.. contents:: Basic Tutorial - Using Core Library to write records

Basic Tutorial - Write Records using Core Library
=================================================

This tutorial shows how to write records using core library.

.. sectnum::

Open a writer
~~~~~~~~~~~~~

Before everything, you have to open a writer to write records.
These are the steps to follow to `open a writer`.

Create distributedlog URI
-------------------------

::

    String dlUriStr = ...;
    URI uri = URI.create(dlUriStr);

Create distributedlog configuration
-----------------------------------

::

    DistributedLogConfiguration conf = new DistributedLogConfiguration();


Enable immediate flush
----------------------

::

    conf.setImmediateFlushEnabled(true);
    conf.setOutputBufferSize(0);
    conf.setPeriodicFlushFrequencyMilliSeconds(0);


Enable immediate locking
------------------------

So if there is already a writer wring to the stream, opening another writer will
fail because previous writer already held a lock.

::

    conf.setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);


Build the distributedlog namespace
----------------------------------

::

    DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
            .conf(conf)
            .uri(uri)
            .regionId(DistributedLogConstants.LOCAL_REGION_ID)
            .clientId("console-writer")
            .build(); 


Open the writer
---------------

::

    DistributedLogManager dlm = namespace.openLog("basic-stream-1");
    AsyncLogWriter writer = FutureUtils.result(dlm.openAsyncLogWriter());


Write Records
~~~~~~~~~~~~~

Once you got a `writer` instance, you can start writing `records` into the stream.

Construct a log record
----------------------

Here lets use `System.currentTimeMillis()` as the `TransactionID`.

::

    byte[] data = ...;
    LogRecord record = new LogRecord(System.currentTimeMillis(), data); 


Write the log record
--------------------

::

    Future<DLSN> writeFuture = writer.write(record);


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


Close the writer
~~~~~~~~~~~~~~~~

Close the writer after usage
----------------------------

::

    FutureUtils.result(writer.asyncClose());


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


Create the stream
-----------------

::

        // Create Stream `basic-stream-1`
        // dlog tool create -u ${distributedlog-uri} -r ${stream-prefix} -e ${stream-regex}
        ./distributedlog-core/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/distributedlog -r basic-stream- -e 1


Tail the stream
---------------

Tailing the stream using `TailReader` to wait for new records.

::

        // Tailing Stream `basic-stream-1`
        // runner run com.twitter.distributedlog.basic.TailReader ${distributedlog-uri} ${stream}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.TailReader distributedlog://127.0.0.1:7000/messaging/distributedlog basic-stream-1


Write records
-------------

Run the example to write records to the stream in a console.

::

        // Write Records into Stream `basic-stream-1`
        // runner run com.twitter.distributedlog.basic.ConsoleWriter ${distributedlog-uri} ${stream}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.ConsoleWriter distributedlog://127.0.0.1:7000/messaging/distributedlog basic-stream-1


Check the results
-----------------

Example output from `ConsoleWriter` and `TailReader`.

::

        // Output of `ConsoleWriter`
        Opening log stream basic-stream-1
        [dlog] > test!
        [dlog] >


        // Output of `TailReader`
        Opening log stream basic-stream-1
        Log stream basic-stream-1 is empty.
        Wait for records starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        """
        test!
        """

Attempt a second writer 
-----------------------

Open another terminal to run `ConsoleWriter`. It would fail with `OwnershipAcquireFailedException` as previous
`ConsoleWriter` is still holding lock on writing to stream `basic-stream-1`.

::

        Opening log stream basic-stream-1
        Exception in thread "main" com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException: LockPath - /messaging/distributedlog/basic-stream-1/<default>/lock: Lock acquisition failed, the current owner is console-writer
            at com.twitter.distributedlog.lock.ZKSessionLock$8.apply(ZKSessionLock.java:570)
            at com.twitter.distributedlog.lock.ZKSessionLock$8.apply(ZKSessionLock.java:567)
            at com.twitter.util.Future$$anonfun$map$1$$anonfun$apply$8.apply(Future.scala:1041)
            at com.twitter.util.Try$.apply(Try.scala:13)
            at com.twitter.util.Future$.apply(Future.scala:132)
            at com.twitter.util.Future$$anonfun$map$1.apply(Future.scala:1041)
            at com.twitter.util.Future$$anonfun$map$1.apply(Future.scala:1040)
            at com.twitter.util.Promise$Transformer.liftedTree1$1(Promise.scala:112)
            at com.twitter.util.Promise$Transformer.k(Promise.scala:112)
            at com.twitter.util.Promise$Transformer.apply(Promise.scala:122)
            at com.twitter.util.Promise$Transformer.apply(Promise.scala:103)
            at com.twitter.util.Promise$$anon$1.run(Promise.scala:357)
            at com.twitter.concurrent.LocalScheduler$Activation.run(Scheduler.scala:178)
            at com.twitter.concurrent.LocalScheduler$Activation.submit(Scheduler.scala:136)
            at com.twitter.concurrent.LocalScheduler.submit(Scheduler.scala:207)
            at com.twitter.concurrent.Scheduler$.submit(Scheduler.scala:92)
            at com.twitter.util.Promise.runq(Promise.scala:350)
            at com.twitter.util.Promise.updateIfEmpty(Promise.scala:716)
            at com.twitter.util.Promise.update(Promise.scala:694)
            at com.twitter.util.Promise.setValue(Promise.scala:670)
            at com.twitter.distributedlog.lock.ZKSessionLock$9.safeRun(ZKSessionLock.java:622)
            at org.apache.bookkeeper.util.SafeRunnable.run(SafeRunnable.java:31)
            at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)
            at java.util.concurrent.FutureTask.run(FutureTask.java:262)
            at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:178)
            at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:292)
            at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
            at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
            at java.lang.Thread.run(Thread.java:745) 
