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
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import jline.ConsoleReader;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Writer write records from console
 */
public class ConsoleWriter {

    private final static String HELP = "ConsoleWriter <uri> <string>";
    private final static String PROMPT_MESSAGE = "[dlog] > ";

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.out.println(HELP);
            return;
        }

        String dlUriStr = args[0];
        final String streamName = args[1];

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setImmediateFlushEnabled(true);
        conf.setOutputBufferSize(0);
        conf.setPeriodicFlushFrequencyMilliSeconds(0);
        conf.setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                .clientId("console-writer")
                .build();

        // open the dlm
        System.out.println("Opening log stream " + streamName);
        DistributedLogManager dlm = namespace.openLog(streamName);

        try {
            AsyncLogWriter writer = null;
            try {
                writer = FutureUtils.result(dlm.openAsyncLogWriter());

                ConsoleReader reader = new ConsoleReader();
                String line;
                while ((line = reader.readLine(PROMPT_MESSAGE)) != null) {
                    writer.write(new LogRecord(System.currentTimeMillis(), line.getBytes(UTF_8)))
                            .addEventListener(new FutureEventListener<DLSN>() {
                                @Override
                                public void onFailure(Throwable cause) {
                                    System.out.println("Encountered error on writing data");
                                    cause.printStackTrace(System.err);
                                    Runtime.getRuntime().exit(0);
                                }

                                @Override
                                public void onSuccess(DLSN value) {
                                    // done
                                }
                            });
                }
            } finally {
                if (null != writer) {
                    FutureUtils.result(writer.asyncClose(), Duration.apply(5, TimeUnit.SECONDS));
                }
            }
        } finally {
            dlm.close();
            namespace.close();
        }
    }

}
