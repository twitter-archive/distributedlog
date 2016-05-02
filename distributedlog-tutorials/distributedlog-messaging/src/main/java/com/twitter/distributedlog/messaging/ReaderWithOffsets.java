package com.twitter.distributedlog.messaging;

import com.twitter.distributedlog.*;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.net.URI;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

/**
 * Reader with offsets
 */
public class ReaderWithOffsets {

    private final static String HELP = "ReaderWithOffsets <uri> <string> <reader-id> <offset-store-file>";

    public static void main(String[] args) throws Exception {
        if (4 != args.length) {
            System.out.println(HELP);
            return;
        }

        String dlUriStr = args[0];
        final String streamName = args[1];
        final String readerId = args[2];
        final String offsetStoreFile = args[3];

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();

        // open the dlm
        System.out.println("Opening log stream " + streamName);
        DistributedLogManager dlm = namespace.openLog(streamName);

        // open the offset store
        Options options = new Options();
        options.createIfMissing(true);
        final DB offsetDB = factory.open(new File(offsetStoreFile), options);
        final AtomicReference<DLSN> lastDLSN = new AtomicReference<DLSN>(null);
        // offset updater
        final ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (null != lastDLSN.get()) {
                    offsetDB.put(readerId.getBytes(UTF_8), lastDLSN.get().serializeBytes());
                    System.out.println("Updated reader " + readerId + " offset to " + lastDLSN.get());
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
        try {
            byte[] offset = offsetDB.get(readerId.getBytes(UTF_8));
            DLSN dlsn;
            if (null == offset) {
                dlsn = DLSN.InitialDLSN;
            } else {
                dlsn = DLSN.deserializeBytes(offset);
            }
            readLoop(dlm, dlsn, lastDLSN);
        } finally {
            offsetDB.close();
            dlm.close();
            namespace.close();
        }
    }

    private static void readLoop(final DistributedLogManager dlm,
                                 final DLSN dlsn,
                                 final AtomicReference<DLSN> lastDLSN)
            throws Exception {

        final CountDownLatch keepAliveLatch = new CountDownLatch(1);

        System.out.println("Wait for records starting from " + dlsn);
        final AsyncLogReader reader = FutureUtils.result(dlm.openAsyncLogReader(dlsn));
        final FutureEventListener<LogRecordWithDLSN> readListener = new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                System.err.println("Encountered error on reading records from stream " + dlm.getStreamName());
                cause.printStackTrace(System.err);
                keepAliveLatch.countDown();
            }

            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                System.out.println("Received record " + record.getDlsn());
                System.out.println("\"\"\"");
                System.out.println(new String(record.getPayload(), UTF_8));
                System.out.println("\"\"\"");
                lastDLSN.set(record.getDlsn());
                reader.readNext().addEventListener(this);
            }
        };
        reader.readNext().addEventListener(readListener);

        keepAliveLatch.await();
        FutureUtils.result(reader.asyncClose(), Duration.apply(5, TimeUnit.SECONDS));
    }

}
