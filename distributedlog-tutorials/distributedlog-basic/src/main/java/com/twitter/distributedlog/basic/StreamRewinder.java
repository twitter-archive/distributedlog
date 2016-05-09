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

import com.twitter.distributedlog.*;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.CountDownLatch;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Rewind a stream to read data back in a while
 */
public class StreamRewinder {

    private final static String HELP = "StreamRewinder <uri> <string> <seconds>";

    public static void main(String[] args) throws Exception {
        if (3 != args.length) {
            System.out.println(HELP);
            return;
        }

        String dlUriStr = args[0];
        final String streamName = args[1];
        final int rewindSeconds = Integer.parseInt(args[2]);

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();

        // open the dlm
        System.out.println("Opening log stream " + streamName);
        DistributedLogManager dlm = namespace.openLog(streamName);

        try {
            readLoop(dlm, rewindSeconds);
        } finally {
            dlm.close();
            namespace.close();
        }
    }

    private static void readLoop(final DistributedLogManager dlm,
                                 final int rewindSeconds) throws Exception {

        final CountDownLatch keepAliveLatch = new CountDownLatch(1);

        long rewindToTxId = System.currentTimeMillis() -
                TimeUnit.MILLISECONDS.convert(rewindSeconds, TimeUnit.SECONDS);

        System.out.println("Record records starting from " + rewindToTxId
                + " which is " + rewindSeconds + " seconds ago");
        final AsyncLogReader reader = FutureUtils.result(dlm.openAsyncLogReader(rewindToTxId));
        final AtomicBoolean caughtup = new AtomicBoolean(false);
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

                long diffInMilliseconds = System.currentTimeMillis() - record.getTransactionId();

                if (!caughtup.get() && diffInMilliseconds < 2000) {
                    System.out.println("Reader caught with latest data");
                    caughtup.set(true);
                }

                reader.readNext().addEventListener(this);
            }
        };
        reader.readNext().addEventListener(readListener);

        keepAliveLatch.await();
        FutureUtils.result(reader.asyncClose(), Duration.apply(5, TimeUnit.SECONDS));
    }

}
