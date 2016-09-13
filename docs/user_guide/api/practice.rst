---
layout: default

# Sub-level navigation
sub-nav-group: user-guide
sub-nav-parent: apis
sub-nav-pos: 5
sub-nav-title: Best Practise

---

.. contents:: Best Practices

Best Practices
==============

Write records using Fat Client or Thin Client
---------------------------------------------

`Fat Client` is the writer in distributedlog core library which talks to ZooKeeper and BookKeeper directly,
while `Thin Client` is the write proxy client which talks to write proxy service.

It is strongly recommended that writing records via `Write Proxy` service rather than using core library directly.
Because using `Thin Client` has following benefits:

- `Thin Client` is purely thrift RPC based client. It doesn't talk to zookeeper and bookkeeper directly and less complicated.
- `Write Proxy` manages ownerships of log writers. `Thin Client` doesn't have to deal with ownerships of log writers.
- `Thin Client` is more upgrade-friendly than `Fat Client`.

The only exception to use distributedlog core library directly is when you application requires:

- Write Ordering. `Write Ordering` means all the writes issued by the writer should be written in a strict order
  in the log. `Write Proxy` service could only guarantee `Read Ordering`. `Read Ordering` means the write proxies will write 
  the write requests in their receiving order and gurantee the data seen by all the readers in same order.
- Ownership Management. If the application already has any kind of ownership management, like `master-slave`, it makes more
  sense that it uses distributedlog core library directly.

How to position reader by time
------------------------------

Sometimes, application wants to read data by time, like read data from 2 hours ago. This could be done by positioning
the reader using `Transaction ID`, if the `Transaction ID` is the timestamp (All the streams produced by `Write Proxy` use
timestamp as `Transaction ID`).

::

    DistributedLogManager dlm = ...;

    long timestamp = System.currentTimeMillis();
    long startTxId = timestamp - TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS);
    AsyncLogReader reader = Await.result(dlm.openAsyncLogReader(startTxId));
    ...

How to seal a stream
--------------------

Typically, DistributedLog is used as endless streams. In some use cases, application wants to `seal` the stream. So writers
can't write more data into the log stream and readers could receive notifications about the stream has been sealed.

Write could seal a log stream as below:

::
    
    DistributedLogManager dlm = ...;

    LogWriter writer = dlm.startLogSegmentNonPartitioned;
    // writer writes bunch of records
    ...

    // writer seals the stream
    writer.markEndOfStream();

Reader could detect a stream has been sealed as below:

::
    
    DistributedLogManager dlm = ...;

    LogReader reader = dlm.getInputStream(1L);
    LogRecord record;
    try {
        while ((record = reader.readNext(false)) != null) {
            // process the record
            ...
        }
    } catch (EndOfStreamException eos) {
        // the stream has been sealed
        ...
    }
