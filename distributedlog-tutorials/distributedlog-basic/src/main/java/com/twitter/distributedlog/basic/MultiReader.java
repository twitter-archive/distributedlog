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
import com.twitter.util.FutureEventListener;
import org.apache.commons.lang.StringUtils;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A reader is tailing multiple streams
 */
public class MultiReader {

    private final static String HELP = "TailReader <uri> <stream-1>[,<stream-2>,...,<stream-n>]";

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.out.println(HELP);
            return;
        }

        String dlUriStr = args[0];
        final String streamList = args[1];

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();

        String[] streamNameList = StringUtils.split(streamList, ',');
        DistributedLogManager[] managers = new DistributedLogManager[streamNameList.length];

        for (int i = 0; i < managers.length; i++) {
            String streamName = streamNameList[i];
            // open the dlm
            System.out.println("Opening log stream " + streamName);
            managers[i] = namespace.openLog(streamName);
        }

        final CountDownLatch keepAliveLatch = new CountDownLatch(1);

        for (DistributedLogManager dlm : managers) {
            final DistributedLogManager manager = dlm;
            dlm.getLastLogRecordAsync().addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                @Override
                public void onFailure(Throwable cause) {
                    if (cause instanceof LogNotFoundException) {
                        System.err.println("Log stream " + manager.getStreamName() + " is not found. Please create it first.");
                        keepAliveLatch.countDown();
                    } else if (cause instanceof LogEmptyException) {
                        System.err.println("Log stream " + manager.getStreamName() + " is empty.");
                        readLoop(manager, DLSN.InitialDLSN, keepAliveLatch);
                    } else {
                        System.err.println("Encountered exception on process stream " + manager.getStreamName());
                        keepAliveLatch.countDown();
                    }
                }

                @Override
                public void onSuccess(LogRecordWithDLSN record) {
                    readLoop(manager, record.getDlsn(), keepAliveLatch);
                }
            });
        }
        keepAliveLatch.await();
        for (DistributedLogManager dlm : managers) {
            dlm.close();
        }
        namespace.close();
    }

    private static void readLoop(final DistributedLogManager dlm,
                                 final DLSN dlsn,
                                 final CountDownLatch keepAliveLatch) {
        System.out.println("Wait for records from " + dlm.getStreamName() + " starting from " + dlsn);
        dlm.openAsyncLogReader(dlsn).addEventListener(new FutureEventListener<AsyncLogReader>() {
            @Override
            public void onFailure(Throwable cause) {
                System.err.println("Encountered error on reading records from stream " + dlm.getStreamName());
                cause.printStackTrace(System.err);
                keepAliveLatch.countDown();
            }

            @Override
            public void onSuccess(AsyncLogReader reader) {
                System.out.println("Open reader to read records from stream " + reader.getStreamName());
                readLoop(reader, keepAliveLatch);
            }
        });
    }

    private static void readLoop(final AsyncLogReader reader,
                                 final CountDownLatch keepAliveLatch) {
        final FutureEventListener<LogRecordWithDLSN> readListener = new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                System.err.println("Encountered error on reading records from stream " + reader.getStreamName());
                cause.printStackTrace(System.err);
                keepAliveLatch.countDown();
            }

            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                System.out.println("Received record " + record.getDlsn() + " from stream " + reader.getStreamName());
                System.out.println("\"\"\"");
                System.out.println(new String(record.getPayload(), UTF_8));
                System.out.println("\"\"\"");
                reader.readNext().addEventListener(this);
            }
        };
        reader.readNext().addEventListener(readListener);
    }

}
