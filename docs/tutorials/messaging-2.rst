---
title: Tutorials - Write records to multiple streams using a load balancer
top-nav-group: messaging
top-nav-pos: 2
top-nav-title: Write records to multiple streams (load balancer)
layout: default
---

.. contents:: Messaging Tutorial - Write records to multiple streams using a load balancer

How to write records to multiple streams using a load balancer
==============================================================

If applications does not care about ordering and they just want use multiple streams to transform messages, it is easier
to use a load balancer to balancing the writes among the multiple streams.

This tutorial shows how to build a multi-streams writer, which ues a finagle load balancer to balancing traffic among multiple streams.

.. sectnum::

Make writing to a stream as a finagle service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to leverage the finagle load balancer to balancing traffic among multiple streams, we have to make writing
to a stream as a finagle service.

::

    class StreamWriter<VALUE> extends Service<VALUE, DLSN> {

        private final String stream;
        private final DistributedLogClient client;

        StreamWriter(String stream,
                     DistributedLogClient client) {
            this.stream = stream;
            this.client = client;
        }

        @Override
        public Future<DLSN> apply(VALUE request) {
            return client.write(stream, ByteBuffer.wrap(request.toString().getBytes(UTF_8)));
        }
    } 


Create a load balancer from multiple streams
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ServiceFactory
-----------------------

Create a set of finagle `ServiceFactory` over multiple streams.

::

        String[] streams;
        Set<ServiceFactory<VALUE, DLSN>> serviceFactories = Sets.newHashSet();
        for (String stream : streams) {
            Service<VALUE, DLSN> service = new StreamWriter(stream, client);
            serviceFactories.add(new SingletonFactory<VALUE, DLSN>(service));
        }


Create the load balancer
------------------------

::

        Service<VALUE, DLSN> writeSerivce =
            Balancers.heap(new scala.util.Random(System.currentTimeMillis()))
                .newBalancer(
                        Activity.value(scalaSet),
                        NullStatsReceiver.get(),
                        new NoBrokersAvailableException("No partitions available")
                ).toService();


Write records
~~~~~~~~~~~~~

Once the balancer service is initialized, we can write records through the balancer service.

::

    Future<DLSN> writeFuture = writeSerivce.write(...);


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

        // Create Stream `messaging-stream-{1,5}`
        // dlog tool create -u ${distributedlog-uri} -r ${stream-prefix} -e ${stream-regex}
        ./distributedlog-core/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/distributedlog -r messaging-stream- -e 1-5


Tail the streams
----------------

Tailing the streams using `MultiReader` to wait for new records.

::

        // Tailing Stream `messaging-stream-{1,5}`
        // runner run com.twitter.distributedlog.basic.MultiReader ${distributedlog-uri} ${stream}[, ${stream}]
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.MultiReader distributedlog://127.0.0.1:7000/messaging/distributedlog messaging-stream-1,messaging-stream-2,messaging-stream-3,messaging-stream-4,messaging-stream-5


Write records
-------------

Run the example to write records to multiple stream in a console.

::

        // Write Records into Stream `messaging-stream-{1,5}`
        // runner run com.twitter.distributedlog.messaging.ConsoleProxyRRMultiWriter ${distributedlog-uri} ${stream}[, ${stream}]
        ./distributedlog-tutorials/distributedlog-messaging/bin/runner run com.twitter.distributedlog.messaging.ConsoleProxyRRMultiWriter 'inet!127.0.0.1:8000' messaging-stream-1,messaging-stream-2,messaging-stream-3,messaging-stream-4,messaging-stream-5


Check the results
-----------------

Example output from `ConsoleProxyRRMultiWriter` and `MultiReader`.

::

        // Output of `ConsoleProxyRRMultiWriter`
        Picked up JAVA_TOOL_OPTIONS: -Dfile.encoding=utf8
        May 08, 2016 1:22:35 PM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[inet] = com.twitter.finagle.InetResolver(com.twitter.finagle.InetResolver@6c4cbf96)
        May 08, 2016 1:22:35 PM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fixedinet] = com.twitter.finagle.FixedInetResolver(com.twitter.finagle.FixedInetResolver@57052dc3)
        May 08, 2016 1:22:35 PM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[neg] = com.twitter.finagle.NegResolver$(com.twitter.finagle.NegResolver$@14ff89d7)
        May 08, 2016 1:22:35 PM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[nil] = com.twitter.finagle.NilResolver$(com.twitter.finagle.NilResolver$@14b28d06)
        May 08, 2016 1:22:35 PM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fail] = com.twitter.finagle.FailResolver$(com.twitter.finagle.FailResolver$@56488f87)
        May 08, 2016 1:22:35 PM com.twitter.finagle.Init$$anonfun$1 apply$mcV$sp
        INFO: Finagle version media-platform-tools/release-20160330-1117-sgerstein-9-g2dcdd6c (rev=2dcdd6c866f9bd3599ed49568d651189735e8ad6) built at 20160330-160058
        [dlog] > message-1
        [dlog] > message-2
        [dlog] > message-3
        [dlog] > message-4
        [dlog] > message-5
        [dlog] >


        // Output of `MultiReader`
        Opening log stream messaging-stream-1
        Opening log stream messaging-stream-2
        Opening log stream messaging-stream-3
        Opening log stream messaging-stream-4
        Opening log stream messaging-stream-5
        Log stream messaging-stream-2 is empty.
        Wait for records from messaging-stream-2 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream messaging-stream-2
        Log stream messaging-stream-1 is empty.
        Wait for records from messaging-stream-1 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream messaging-stream-1
        Log stream messaging-stream-3 is empty.
        Wait for records from messaging-stream-3 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream messaging-stream-3
        Log stream messaging-stream-4 is empty.
        Wait for records from messaging-stream-4 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream messaging-stream-4
        Log stream messaging-stream-5 is empty.
        Wait for records from messaging-stream-5 starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Open reader to read records from stream messaging-stream-5
        Received record DLSN{logSegmentSequenceNo=1, entryId=2, slotId=0} from stream messaging-stream-3
        """
        message-1
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=2, slotId=0} from stream messaging-stream-2
        """
        message-2
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=4, slotId=0} from stream messaging-stream-2
        """
        message-3
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=2, slotId=0} from stream messaging-stream-4
        """
        message-4
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=6, slotId=0} from stream messaging-stream-2
        """
        message-5
        """
