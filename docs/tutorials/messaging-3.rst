---
title: Tutorials - At-least-once Processing
top-nav-group: messaging
top-nav-pos: 3
top-nav-title: At-least-once Processing
layout: default
---

.. contents:: Messaging Tutorial - At-least-once Processing

At-least-once Processing
========================

Applications typically choose between `at-least-once` and `exactly-once` processing semantics.
`At-least-once` processing guarantees that the application will process all the log records,
however when the application resumes after failure, previously processed records may be re-processed
if they have not been acknowledged. With at least once processing guarantees the application can store
reader positions in an external store and update it periodically. Upon restart the application will
reprocess messages since the last updated reader position.

This tutorial shows how to do `at-least-once` processing by using a `offset-store` to track the reading positions.

.. sectnum::

How to track reading positions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Applications typically choose an external storage (e.g key/value storage) or another log stream to store their
read positions. In this example, we used a local key/value store - `LevelDB` to store the read positions.

Open the offset store
---------------------

::

        String offsetStoreFile = ...;
        Options options = new Options();
        options.createIfMissing(true);
        DB offsetDB = factory.open(new File(offsetStoreFile), options);


Read the reader read position
-----------------------------

Read the reader read position from the offset store.

::

        byte[] offset = offsetDB.get(readerId.getBytes(UTF_8));
        DLSN dlsn;
        if (null == offset) {
            dlsn = DLSN.InitialDLSN;
        } else {
            dlsn = DLSN.deserializeBytes(offset);
        }


Read records
------------

Start read from the read position that recorded in offset store.

::

        final AsyncLogReader reader = FutureUtils.result(dlm.openAsyncLogReader(dlsn));


Track read position
-------------------

Track the last read position while reading using `AtomicReference`.

::

        final AtomicReference<DLSN> lastReadDLSN = new AtomicReference<DLSN>(null);
        reader.readNext().addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
            ...

            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                lastReadDLSN.set(record.getDlsn()); 
                // process the record
                ...
                // read next record
                reader.readNext().addEventListener(this);
            }
        });


Record read position
--------------------

Periodically update the last read position to the offset store.

::

        final ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (null != lastDLSN.get()) {
                    offsetDB.put(readerId.getBytes(UTF_8), lastDLSN.get().serializeBytes());
                }
            }
        }, 10, 10, TimeUnit.SECONDS);


Check `distributedlog-tutorials/distributedlog-messaging/src/main/java/com/twitter/distributedlog/messaging/ReaderWithOffsets`_ for more details.

.. _distributedlog-tutorials/distributedlog-messaging/src/main/java/com/twitter/distributedlog/messaging/ReaderWithOffsets: https://github.com/apache/incubator-distributedlog/blob/master/distributedlog-tutorials/distributedlog-messaging/src/main/java/com/twitter/distributedlog/messaging/ReaderWithOffsets.java
