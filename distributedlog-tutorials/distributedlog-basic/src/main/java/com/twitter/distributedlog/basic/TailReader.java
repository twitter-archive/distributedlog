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
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A reader is tailing a log
 */
public class TailReader {

    private final static String HELP = "TailReader <uri> <string>";

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.out.println(HELP);
            return;
        }

        String dlUriStr = args[0];
        final String streamName = args[1];

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();

        // open the dlm
        System.out.println("Opening log stream " + streamName);
        DistributedLogManager dlm = namespace.openLog(streamName);

        // get the last record
        LogRecordWithDLSN lastRecord;
        DLSN dlsn;
        try {
            lastRecord = dlm.getLastLogRecord();
            dlsn = lastRecord.getDlsn();
            readLoop(dlm, dlsn);
        } catch (LogNotFoundException lnfe) {
            System.err.println("Log stream " + streamName + " is not found. Please create it first.");
            return;
        } catch (LogEmptyException lee) {
            System.err.println("Log stream " + streamName + " is empty.");
            dlsn = DLSN.InitialDLSN;
            readLoop(dlm, dlsn);
        } finally {
            dlm.close();
            namespace.close();
        }
    }

    private static void readLoop(final DistributedLogManager dlm,
                                 final DLSN dlsn) throws Exception {

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
                reader.readNext().addEventListener(this);
            }
        };
        reader.readNext().addEventListener(readListener);

        keepAliveLatch.await();
        FutureUtils.result(reader.asyncClose(), Duration.apply(5, TimeUnit.SECONDS));
    }

}
