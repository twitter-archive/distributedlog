### Rewind reading records by time

This tutorial shows how to rewind reading data from a stream by time.

#### Open a distributedlog manager

-   Create distributedlog URI.
    ```
        String dlUriStr = ...;
        URI uri = URI.create(dlUriStr);
    ```

-   Create distributedlog configuration.
    ```
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
    ```

-   Build the distributedlog namespace.
    ```
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build(); 
    ```

-   Open the distributedlog manager.
    ```
        DistributedLogManager dlm = namespace.openLog("basic-stream-10");
    ```

#### Rewind the stream

-   Position the reader using timestamp. Since the records written by write proxy will be assigned
    `System.currentTimeMillis()` as the `TransactionID`. It is straightforward to use `TransactionID`
    to rewind reading the records.
    ```
        int rewindSeconds = 60; // 60 seconds
        long fromTxID = System.currentTimeMillis() -
                TimeUnit.MILLISECONDS.convert(rewindSeconds, TimeUnit.SECONDS);
        AsyncLogReader reader = FutureUtils.result(dlm.openAsyncLogReader(fromTxID));
    ```

#### Read Records

-   Read the next available record from the stream. The future is satisified when the record is available.
    ```
        Future<LogRecordWithDLSN> readFuture = reader.readNext();
    ```

-   Register a future listener on read completion.
    ```
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
    ```

#### Close the reader

-   Close the reader after usage.
    ```
        FutureUtils.result(reader.asyncClose());
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
        // Create Stream `basic-stream-10`
        // dlog tool create -u ${distributedlog-uri} -r ${stream-prefix} -e ${stream-regex}
        ./distributedlog-core/bin/dlog tool create -u distributedlog://127.0.0.1:7000/messaging/distributedlog -r basic-stream- -e 10
    ```

4.  Run the `RecordGenerator` to generate records.
    ```
        // Write Records into Stream `basic-stream-10` in 1 requests/second
        // runner run com.twitter.distributedlog.basic.RecordGenerator ${distributedlog-uri} ${stream} ${rate}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.RecordGenerator 'inet!127.0.0.1:8000' basic-stream-10 1
    ```

5.  Rewind the stream using `StreamRewinder` to read records from 30 seconds ago
    ```
        // Rewind `basic-stream-10`
        // runner run com.twitter.distributedlog.basic.StreamRewinder ${distributedlog-uri} ${stream} ${seconds-to-rewind}
        ./distributedlog-tutorials/distributedlog-basic/bin/runner run com.twitter.distributedlog.basic.StreamRewinder distributedlog://127.0.0.1:7000/messaging/distributedlog basic-stream-10  30
    ```

6.  Example output from `StreamRewinder`.
    ```
        // Output of `StreamRewinder`
        Opening log stream basic-stream-10
        Record records starting from 1462736697481 which is 30 seconds ago
        Received record DLSN{logSegmentSequenceNo=1, entryId=264, slotId=0}
        """
        record-1462736697685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=266, slotId=0}
        """
        record-1462736698684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=268, slotId=0}
        """
        record-1462736699684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=270, slotId=0}
        """
        record-1462736700686
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=272, slotId=0}
        """
        record-1462736701685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=274, slotId=0}
        """
        record-1462736702684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=276, slotId=0}
        """
        record-1462736703683
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=278, slotId=0}
        """
        record-1462736704685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=280, slotId=0}
        """
        record-1462736705686
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=282, slotId=0}
        """
        record-1462736706682
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=284, slotId=0}
        """
        record-1462736707685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=286, slotId=0}
        """
        record-1462736708686
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=288, slotId=0}
        """
        record-1462736709684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=290, slotId=0}
        """
        record-1462736710684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=292, slotId=0}
        """
        record-1462736711686
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=294, slotId=0}
        """
        record-1462736712686
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=296, slotId=0}
        """
        record-1462736713684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=298, slotId=0}
        """
        record-1462736714682
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=300, slotId=0}
        """
        record-1462736715685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=302, slotId=0}
        """
        record-1462736716684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=304, slotId=0}
        """
        record-1462736717684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=306, slotId=0}
        """
        record-1462736718684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=308, slotId=0}
        """
        record-1462736719685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=310, slotId=0}
        """
        record-1462736720683
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=312, slotId=0}
        """
        record-1462736721686
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=314, slotId=0}
        """
        record-1462736722685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=316, slotId=0}
        """
        record-1462736723683
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=318, slotId=0}
        """
        record-1462736724683
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=320, slotId=0}
        """
        record-1462736725685
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=322, slotId=0}
        """
        record-1462736726686
        """
        Reader caught with latest data
        Received record DLSN{logSegmentSequenceNo=1, entryId=324, slotId=0}
        """
        record-1462736727686
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=326, slotId=0}
        """
        record-1462736728684
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=328, slotId=0}
        """
        record-1462736729682
        """
        Received record DLSN{logSegmentSequenceNo=1, entryId=330, slotId=0}
        """
        record-1462736730685
        """
    ```
