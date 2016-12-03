/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.basic;

import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.thrift.ClientId$;
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
                .clientId(ClientId$.MODULE$.apply("record-generator"))
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
