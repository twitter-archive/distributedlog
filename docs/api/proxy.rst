Write Proxy Client API
======================

`Write Proxy` is a 'stateless' service on managing the ownerships of writers of log streams. It is used to
accept to `fan-in` writes from different publishers.

Build Client
------------
 
The first thing of using `Write Proxy` service is to build the write proxy client. The endpoint of a `Write Proxy` service
is typically identified by `Finagle Name`_. Name strings must be supplied when constructing a `Write Proxy` client.

.. _Finagle Name: http://twitter.github.io/finagle/guide/Names.html

::

    // 1. Create a Finagle client builder. It would be used for building connection to write proxies.
    ClientBuilder clientBuilder = ClientBuilder.get()
        .hostConnectionLimit(1)
        .hostConnectionCoresize(1)
        .tcpConnectTimeout(Duration$.MODULE$.fromMilliseconds(200))
        .connectTimeout(Duration$.MODULE$.fromMilliseconds(200))
        .requestTimeout(Duration$.MODULE$.fromSeconds(2));

    // 2. Choose a client id to identify the client.
    ClientId clientId = ClientId$.MODULE$.apply("test");

    String finagleNameStr = "inet!127.0.0.1:8000";
    
    // 3. Create the write proxy client builder
    DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder()
        .name("test-writer")
        .clientId(clientId)
        .clientBuilder(clientBuilder)
        .statsReceiver(statsReceiver)
        .finagleNameStr(finagleNameStr);

    // 4. Build the client
    DistributedLogClient client = builder.build();

Write Records
-------------

Writing records to log streams via `Write Proxy` is much simpler than using the core library. The transaction id
will be automatically assigned with `timestamp` by write proxies. The `timestamp` is guaranteed to be non-decreasing, which it
could be treated as `physical time` within a log stream, and be used for implementing features like `TTL` in a strong consistent
database.

::
    
    DistributedLogClient client = ...;

    // Write a record to a stream
    String streamName = "test-stream";
    byte[] data = ...;
    Future<DLSN> writeFuture = client.write(streamName, ByteBuffer.wrap(data));
    Await.result(writeFuture);

Truncate Streams
----------------

Client could issue truncation requests (via `#truncate(String, DLSN)`) to write proxies to truncate a log stream up to a given
position.

::

    DistributedLogClient client = ...;

    // Truncate a stream to DLSN
    String streamName = "test-stream";
    DLSN truncationDLSN = ...;
    Future<DLSN> truncateFuture = client.truncate(streamName, truncationDLSN);
    Await.result(truncateFuture);
    
