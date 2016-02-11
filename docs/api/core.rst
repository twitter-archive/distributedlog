Core Library API
================

The distributedlog core library interacts with namespaces and logs directly.
It is written in Java.

Namespace API
-------------

A DL namespace is a collection of *log streams*. Applications could *create*
or *delete* logs under a DL namespace.

Namespace URI
~~~~~~~~~~~~~

An **URI** is used to locate the *namespace*. The *Namespace URI* is typically
comprised of *3* components:

* scheme: `distributedlog-<backend>`. The *backend* indicates what backend is used to store the log data.
* domain name: the domain name that used to talk to the *backend*. In the example as below, the domain name part is *zookeeper server*, which is used to store log metadata in bookkeeper based backend implementation.
* path: path points to the location that stores logs. In the example as below, it is a zookeeper path that points to the znode that stores all the logs metadata.

::

    distributedlog-bk://<zookeeper-server>/path/to/stream

The available backend is only bookkeeper based backend.
The default `distributedlog` scheme is aliased to `distributedlog-bk`.

Building a Namespace
~~~~~~~~~~~~~~~~~~~~

Once you have the *namespace uri*, you could build the namespace instance.
The namespace instance will be used for operating streams under it.

::

    // DistributedLog Configuration
    DistributedLogConfiguration conf = new DistributedLogConfiguration();
    // Namespace URI
    URI uri = ...; // create the namespace uri
    // create a builder to build namespace instances
    DistributedLogNamespaceBuilder builder = DistributedLogNamespaceBuilder.newBuilder();
    DistributedLogNamespace namespace = builder
        .conf(conf)             // configuration that used by namespace
        .uri(uri)               // namespace uri
        .statsLogger(...)       // stats logger to log stats
        .featureProvider(...)   // feature provider on controlling features
        .build();

Create a Log
~~~~~~~~~~~~

Creating a log is pretty straight forward by calling `distributedlognamespace#createlog(logname)`.
it only creates the log under the namespace but doesn't return any handle for operating the log.

::

    DistributedLogNamespace namespace = ...; // namespace
    try {
        namespace.createLog("test-log");
    } catch (IOException ioe) {
        // handling the exception on creating a log
    }

Open a Log
~~~~~~~~~~

A `DistributedLogManager` handle will be returned when opening a log by `#openLog(logName)`. The
handle could be used for writing data to or reading data from the log. If the log doesn't exist
and `createStreamIfNotExists` is set to true in the configuration, the log will be created
automatically when writing first record.

::

    DistributedLogConfiguration conf = new DistributedLogConfiguration();
    conf.setCreateStreamIfNotExists(true);
    DistributedLogNamespace namespace = DistributedLogNamespace.newBuilder()
        .conf(conf)
        ...
        .build();
    DistributedLogManager logManager = namespace.openLog("test-log");
    // use the log manager to open writer to write data or open reader to read data
    ...

Sometimes, applications may open a log with different configuration settings. It could be done via
a overloaded `#openLog` method, as below:

::

    DistributedLogConfiguration conf = new DistributedLogConfiguration();
    // set the retention period hours to 24 hours.
    conf.setRetentionPeriodHours(24);
    URI uri = ...;
    DistributedLogNamespace namespace = DistributedLogNamespace.newBuilder()
        .conf(conf)
        .uri(uri)
        ...
        .build();

    // Per Log Configuration
    DistributedLogConfigration logConf = new DistributedLogConfiguration();
    // set the retention period hours to 12 hours for a single stream
    logConf.setRetentionPeriodHours(12);

    // open the log with overrided settings
    DistributedLogManager logManager = namespace.openLog("test-log",
        Optional.of(logConf),
        Optiona.absent());

Delete a Log
~~~~~~~~~~~~

`DistributedLogNamespace#deleteLog(logName)` will deletes the log from the namespace. Deleting a log
will attempt acquiring a lock before deletion. If a log is writing by an active writer, the lock
would be already acquired by the writer. so the deleting will fail.

::

    DistributedLogNamespace namespace = ...;
    try {
        namespace.deleteLog("test-log");
    } catch (IOException ioe) {
        // handle the exceptions
    }

Log Existence
~~~~~~~~~~~~~

Applications could check whether a log exists in a namespace by calling `DistributedLogNamespace#logExists(logName)`.

::

    DistributedLogNamespace namespace = ...;
    if (namespace.logExists("test-log")) {
        // actions when log exists
    } else {
        // actions when log doesn't exist
    }

Get List of Logs
~~~~~~~~~~~~~~~~

Applications could list the logs under a namespace by calling `DistributedLogNamespace#getLogs()`.

::

    DistributedLogNamespace namespace = ...;
    Iterator<String> logs = namespace.getLogs();
    while (logs.hasNext()) {
        String logName = logs.next();
        // ... process the log
    }

Writer API
----------

There are two ways to write records into a log stream, one is using 'synchronous' `LogWriter`, while the other one is using
asynchronous `AsyncLogWriter`.

LogWriter
~~~~~~~~~

The first thing to write data into a log stream is to construct the writer instance. Please note that the distributedlog core library enforce single-writer
semantic by deploying a zookeeper locking mechanism. If there is only an active writer, the subsequent calls to `#startLogSegmentNonPartitioned()` will
fail with `OwnershipAcquireFailedException`.

::
    
    DistributedLogNamespace namespace = ....;
    DistributedLogManager dlm = namespace.openLog("test-log");
    LogWriter writer = dlm.startLogSegmentNonPartitioned();

.. _Construct Log Record:

Log records are constructed to represent the data written to a log stream. Each log record is associated with application defined transaction id.
The transaction id has to be non-decreasing otherwise writing the record will be rejected with `TransactionIdOutOfOrderException`. Application is allowed to
bypass the transaction id sanity checking by setting `maxIdSanityCheck` to false in configuration. System time and atomic numbers are good candicates used for
transaction id.

::

    long txid = 1L;
    byte[] data = ...;
    LogRecord record = new LogRecord(txid, data);

Application could either add a single record (via `#write(LogRecord)`) or a bunch of records (via `#writeBulk(List<LogRecord>)`) into the log stream.

::

    writer.write(record);
    // or
    List<LogRecord> records = Lists.newArrayList();
    records.add(record);
    writer.writeBulk(records);

The write calls return immediately after the records are added into the output buffer of writer. So the data isn't guaranteed to be durable until writer
explicitly calls `#setReadyToFlush()` and `#flushAndSync()`. Those two calls will first transmit buffered data to backend, wait for transmit acknowledges
and commit the written data to make them visible to readers.

::

    // flush the records
    writer.setReadyToFlush();
    // commit the records to make them visible to readers
    writer.flushAndSync();

The DL log streams are endless streams unless they are sealed. 'endless' means that writers keep writing records to the log streams, readers could keep
tailing reading from the end of the streams and it never stops. Application could seal a log stream by calling `#markEndOfStream()`.

::

    // seal the log stream
    writer.markEndOfStream();
    

The complete example of writing records is showed as below.

::

    DistributedLogNamespace namespace = ....;
    DistributedLogManager dlm = namespace.openLog("test-log");

    LogWriter writer = dlm.startLogSegmentNonPartitioned();
    for (long txid = 1L; txid <= 100L; txid++) {
        byte[] data = ...;
        LogRecord record = new LogRecord(txid, data);
        writer.write(record);
    }
    // flush the records
    writer.setReadyToFlush();
    // commit the records to make them visible to readers
    writer.flushAndSync();

    // seal the log stream
    writer.markEndOfStream();

AsyncLogWriter
~~~~~~~~~~~~~~

Constructing an asynchronous `AsyncLogWriter` is as simple as synchronous `LogWriter`.

::

    DistributedLogNamespace namespace = ....;
    DistributedLogManager dlm = namespace.openLog("test-log");
    AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();

All the writes to `AsyncLogWriter` are asynchronous. The futures representing write results are only satisfied when the data are persisted in the stream durably.
A DLSN (distributedlog sequence number) will be returned for each write, which is used to represent the position (aka offset) of the record in the log stream.
All the records adding in order are guaranteed to be persisted in order.

.. _Async Write Records:

::

    List<Future<DLSN>> addFutures = Lists.newArrayList();
    for (long txid = 1L; txid <= 100L; txid++) {
        byte[] data = ...;
        LogRecord record = new LogRecord(txid, data);
        addFutures.add(writer.write(record));
    }
    List<DLSN> addResults = Await.result(Future.collect(addFutures));

The `AsyncLogWriter` also provides the method to truncate a stream to a given DLSN. This is super helpful for building replicated state machines, who need
explicit controls on when the data could be deleted.

::
    
    DLSN truncateDLSN = ...;
    Future<DLSN> truncateFuture = writer.truncate(truncateDLSN);
    // wait for truncation result
    Await.result(truncateFuture);

Reader API
----------

Sequence Numbers
~~~~~~~~~~~~~~~~

A log record is associated with sequence numbers. First of all, application can assign its own sequence number (called `TransactionID`)
to the log record while writing it (see `Construct Log Record`_). Secondly, a log record will be assigned with an unique system generated sequence number
`DLSN` (distributedlog sequence number) when it is written to a log (see `Async Write Records`_). Besides `DLSN` and `TransactionID`,
a monotonically increasing 64-bits `SequenceId` is assigned to the record at read time, indicating its position within the log.

:Transaction ID: Transaction ID is a positive 64-bits long number that is assigned by the application.
    Transaction ID is very helpful when application wants to organize the records and position the readers using their own sequencing method. A typical
    use case of `Transaction ID` is `DistributedLog Write Proxy`. The write proxy assigns non-decreasing timestamps to log records, which the timestamps
    could be used as `physical time` to implement `TTL` (Time To Live) feature in a strong consistent database.
:DLSN: DLSN (DistributedLog Sequence Number) is the sequence number generated during written time.
    DLSN is comparable and could be used to figure out the order between records. A DLSN is comprised with 3 components. They are `Log Segment Sequence Number`,
    `Entry Id` and `Slot Id`. The DLSN is usually used for comparison, positioning and truncation.
:Sequence ID: Sequence ID is introduced to address the drawback of `DLSN`, in favor of answering questions like `how many records written between two DLSNs`.
    Sequence ID is a 64-bits monotonic increasing number starting from zero. The sequence ids are computed during reading, and only accessible by readers.
    That means writers don't know the sequence ids of records at the point they wrote them.

The readers could be positioned to start reading from any positions in the log, by using `DLSN` or `Transaction ID`.

LogReader
~~~~~~~~~

`LogReader` is a 'synchronous' sequential reader reading records from a log stream starting from a given position. The position could be
`DLSN` (via `#getInputStream(DLSN)`) or `Transaction ID` (via `#getInputStream(long)`). After the reader is open, it could call either
`#readNext(boolean)` or `#readBulk(boolean, int)` to read records out of the log stream sequentially. Closing the reader (via `#close()`)
will release all the resources occupied by this reader instance.

Exceptions could be thrown during reading records due to various issues. Once the exception is thrown, the reader is set to an error state
and it isn't usable anymore. It is the application's responsibility to handle the exceptions and re-create readers if necessary.

::
    
    DistributedLogManager dlm = ...;
    long nextTxId = ...;
    LogReader reader = dlm.getInputStream(nextTxId);

    while (true) { // keep reading & processing records
        LogRecord record;
        try {
            record = reader.readNext(false);
            nextTxId = record.getTransactionId();
            // process the record
            ...
        } catch (IOException ioe) {
            // handle the exception
            ...
            reader = dlm.getInputStream(nextTxId + 1);
        }
    }

Reading records from an endless log stream in `synchronous` way isn't as trivial as in `asynchronous` way. Because it lacks of callback mechanism.
Instead, `LogReader` introduces a flag `nonBlocking` on controlling the waiting behavior on `synchronous` reads. Blocking (`nonBlocking = false`)
means the reads will wait for records before returning read calls, while NonBlocking (`nonBlocking = true`) means the reads will only check readahead
cache and return whatever records available in readahead cache.

The `waiting` period varies in `blocking` mode. If the reader is catching up with writer (there are plenty of records in the log), the read call will
wait until records are read and returned. If the reader is already caught up with writer (there are no more records in the log at read time), the read
call will wait for a small period of time (defined in `DistributedLogConfiguration#getReadAheadWaitTime()`) and return whatever records available in
readahead cache. In other words, if a reader sees no record on blocking reads, it means the reader is `caught-up` with the writer.

See examples below on how to read records using `LogReader`.

::

    // Read individual records
    
    LogReader reader = ...;
    // keep reading records in blocking mode until no records available in the log
    LogRecord record = reader.readNext(false);
    while (null != record) {
        // process the record
        ...
        // read next record
        record = reader.readNext(false);
    }
    ...

    // reader is caught up with writer, doing non-blocking reads to tail the log
    while (true) {
        record = reader.readNext(true);
        if (null == record) {
            // no record available yet. backoff ?
            ...
        } else {
            // process the new record
            ...
        }
    }

::
    
    // Read records in batch

    LogReader reader = ...;
    int N = 10;

    // keep reading N records in blocking mode until no records available in the log
    List<LogRecord> records = reader.readBulk(false, N);
    while (!records.isEmpty()) {
        // process the list of records
        ...
        if (records.size() < N) { // no more records available in the log
            break;
        }
        // read next N records
        records = reader.readBulk(false, N);
    }

    ...

    // reader is caught up with writer, doing non-blocking reads to tail the log
    while (true) {
        records = reader.readBulk(true, N);
        // process the new records
        ...
    }


AsyncLogReader
~~~~~~~~~~~~~~

Similar as `LogReader`, applications could open an `AsyncLogReader` by positioning to different positions, either `DLSN` or `Transaction ID`.

::
    
    DistributedLogManager dlm = ...;

    Future<AsyncLogReader> openFuture;

    // position the reader to transaction id `999`
    openFuture = dlm.openAsyncLogReader(999L);

    // or position the reader to DLSN
    DLSN fromDLSN = ...;
    openFuture = dlm.openAsyncLogReader(fromDLSN);

    AsyncLogReader reader = Await.result(openFuture);

Reading records from an `AsyncLogReader` is asynchronously. The future returned by `#readNext()`, `#readBulk(int)` or `#readBulk(int, long, TimeUnit)`
represents the result of the read operation. The future is only satisfied when there are records available. Application could chain the futures
to do sequential reads.

Reading records one by one from an `AsyncLogReader`.

::

    void readOneRecord(AsyncLogReader reader) {
        reader.readNext().addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
            public void onSuccess(LogRecordWithDLSN record) {
                // process the record
                ...
                // read next
                readOneRecord(reader);
            }
            public void onFailure(Throwable cause) {
                // handle errors and re-create reader
                ...
                reader = ...;
                // read next
                readOneRecord(reader);
            }
        });
    }
    
    AsyncLogReader reader = ...;
    readOneRecord(reader);

Reading records in batches from an `AsyncLogReader`.

::

    void readBulk(AsyncLogReader reader, int N) {
        reader.readBulk(N).addEventListener(new FutureEventListener<List<LogRecordWithDLSN>>() {
            public void onSuccess(List<LogRecordWithDLSN> records) {
                // process the records
                ...
                // read next
                readBulk(reader, N);
            }
            public void onFailure(Throwable cause) {
                // handle errors and re-create reader
                ...
                reader = ...;
                // read next
                readBulk(reader, N);
            }
        });
    }
    
    AsyncLogReader reader = ...;
    readBulk(reader, N);

