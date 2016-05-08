### Write Multi Records Atomic using Write Proxy Client

This tutorial shows how to write multi records atomic using write proxy client.

#### Open a write proxy client

-   Create write proxy client builder.
    ```
        DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder();
                .clientId(ClientId.apply("atomic-writer"))
                .name("atomic-writer");
    ```

-   Enable thrift mux.
    ```
        builder = builder.thriftmux(true);
    ```

-   Point the client to write proxy using finagle name.
    ```
        String finagleNameStr = "inet!127.0.0.1:8000";
        builder = builder.finagleNameStr(finagleNameStr);
    ```

-   Build the write proxy client.
    ```
        DistributedLogClient client = builder.build();
    ```

#### Write Records

-   Create a `RecordSet` for multiple records. The RecordSet has initial 16KB buffer and its
    compression codec is `NONE`.
    ```
        LogRecordSet.Writer recordSetWriter = LogRecordSet.newWriter(16 * 1024, Type.NONE);
    ```

-   Write multiple records into the `RecordSet`.
    ```
        for (String msg : messages) {
            ByteBuffer msgBuf = ByteBuffer.wrap(msg.getBytes(UTF_8));
            Promise<DLSN> writeFuture = new Promise<DLSN>();
            recordSetWriter.writeRecord(msgBuf, writeFuture);
        }
    ```

-   Write the `RecordSet` to a stream.
    ```
        String streamName = "basic-stream-8";
        Future<DLSN> writeFuture = client.writeRecordSet(streamName, recordSetWriter);
    ```

-   Register a future listener on write completion.
    ```
        writeFuture.addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                // executed when write failed.
                recordSetWriter.abortTransmit(cause);
            }

            @Override
            public void onSuccess(DLSN value) {
                // executed when write completed.
                recordSetWriter.completeTransmit(
                        dlsn.getLogSegmentSequenceNo(),
                        dlsn.getEntryId(),
                        dlsn.getSlotId());
            }
        });
    ```

#### Close the write proxy client

-   Close the write proxy client after usage.
    ```
        client.close();
    ```

#### Usage

Run the example in the following steps:

1.  Start the local bookkeeper cluster. After the bookkeeper cluster is started, you could access
    it using distributedlog uri *distributedlog://127.0.0.1:7000/messaging/distributedlog*.

    ```
        // dlog local ${zk-port}
        ./distributedlog-core/bin/dlog local 7000
    ```

2.  Start the write proxy, listening on port 8000.
    ```
        // DistributedLogServerApp -p ${service-port} --shard-id ${shard-id} -sp ${stats-port} -u {distributedlog-uri} -mx -c ${conf-file}
        ./distributedlog-service/bin/dlog com.twitter.distributedlog.service.DistributedLogServerApp -p 8000 --shard-id 1 -sp 8001 -u distributedlog://127.0.0.1:7000/messaging/distributedlog -mx -c ${distributedlog-repo}/distributedlog-service/conf/distributedlog_proxy.conf
    ```

3.  Create the stream under the distributedlog uri.

    ```
        // Create Stream `basic-stream-8`
        // dlog tool create -u ${distributedlog-uri} -r ${stream-prefix} -e ${stream-regex}
        ./distributedlog-core/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/distributedlog -r basic-stream- -e 8
    ```

4.  Tailing the stream using `TailReader` to wait for new records.
    ```
        // Tailing Stream `basic-stream-8`
        // runner run com.twitter.distributedlog.basic.TailReader ${distributedlog-uri} ${stream}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.TailReader distributedlog://127.0.0.1:7000/messaging/distributedlog basic-stream-8
    ```

6.  Run the example to write multiple records to the stream.
    ```
        // Write Records into Stream `basic-stream-8`
        // runner run com.twitter.distributedlog.basic.AtomicWriter ${distributedlog-uri} ${stream} ${message}[, ${message}]
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.AtomicWriter 'inet!127.0.0.1:8000' basic-stream-8 "message-1" "message-2" "message-3" "message-4" "message-5"
    ```

7.  Example output from `AtomicWriter` and `TailReader`.
    ```
        // Output of `AtomicWriter`
        May 08, 2016 11:48:19 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[inet] = com.twitter.finagle.InetResolver(com.twitter.finagle.InetResolver@6c3e459e)
        May 08, 2016 11:48:19 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fixedinet] = com.twitter.finagle.FixedInetResolver(com.twitter.finagle.FixedInetResolver@4d5698f)
        May 08, 2016 11:48:19 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[neg] = com.twitter.finagle.NegResolver$(com.twitter.finagle.NegResolver$@57052dc3)
        May 08, 2016 11:48:19 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[nil] = com.twitter.finagle.NilResolver$(com.twitter.finagle.NilResolver$@14ff89d7)
        May 08, 2016 11:48:19 AM com.twitter.finagle.BaseResolver$$anonfun$resolvers$1 apply
        INFO: Resolver[fail] = com.twitter.finagle.FailResolver$(com.twitter.finagle.FailResolver$@14b28d06)
        May 08, 2016 11:48:19 AM com.twitter.finagle.Init$$anonfun$1 apply$mcV$sp
        Write 'message-1' as record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Write 'message-2' as record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=1}
        Write 'message-3' as record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=2}
        Write 'message-4' as record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=3}
        Write 'message-5' as record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=4}


        // Output of `TailReader`
        Opening log stream basic-stream-8
        Log stream basic-stream-8 is empty.
        Wait for records starting from DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=0}
        """
        message-1
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=1}
        """
        message-2
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=2}
        """
        message-3
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=3}
        """
        message-4
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=0, slotId=4}
        """
        message-5
        """
    ```
