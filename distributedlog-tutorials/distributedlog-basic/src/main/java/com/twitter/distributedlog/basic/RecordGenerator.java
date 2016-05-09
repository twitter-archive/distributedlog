package com.twitter.distributedlog.basic;

import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.FutureEventListener;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Generate records in a given rate
 */
public class RecordGenerator {

    private final static String HELP = "RecordGenerator <finagle-name> <stream> <rate>";

    public static void main(String[] args) throws Exception {
        if (3 != args.length) {
            System.out.println(HELP);
            return;
        }

        String finagleNameStr = args[0];
        final String streamName = args[1];
        double rate = Double.parseDouble(args[2]);
        RateLimiter limiter = RateLimiter.create(rate);

        DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                .clientId(ClientId.apply("record-generator"))
                .name("record-generator")
                .thriftmux(true)
                .finagleNameStr(finagleNameStr)
                .build();

        final CountDownLatch keepAliveLatch = new CountDownLatch(1);
        final AtomicLong numWrites = new AtomicLong(0);
        final AtomicBoolean running = new AtomicBoolean(true);

        while (running.get()) {
            limiter.acquire();
            String record = "record-" + System.currentTimeMillis();
            client.write(streamName, ByteBuffer.wrap(record.getBytes(UTF_8)))
                    .addEventListener(new FutureEventListener<DLSN>() {
                        @Override
                        public void onFailure(Throwable cause) {
                            System.out.println("Encountered error on writing data");
                            cause.printStackTrace(System.err);
                            running.set(false);
                            keepAliveLatch.countDown();
                        }

                        @Override
                        public void onSuccess(DLSN value) {
                            long numSuccesses = numWrites.incrementAndGet();
                            if (numSuccesses % 100 == 0) {
                                System.out.println("Write " + numSuccesses + " records.");
                            }
                        }
                    });
        }

        keepAliveLatch.await();
        client.close();
    }

}
