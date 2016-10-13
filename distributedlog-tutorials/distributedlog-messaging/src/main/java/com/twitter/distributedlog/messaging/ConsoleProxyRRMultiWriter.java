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
package com.twitter.distributedlog.messaging;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.FutureEventListener;
import jline.ConsoleReader;
import jline.Terminal;
import org.apache.commons.lang.StringUtils;

/**
 * Writer write records into multiple streams
 */
public class ConsoleProxyRRMultiWriter {

    private final static String HELP = "ConsoleProxyRRMultiWriter <finagle-name> <stream-1>[,<stream-2>,...,<stream-n>]";
    private final static String PROMPT_MESSAGE = "[dlog] > ";

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.out.println(HELP);
            return;
        }

        String finagleNameStr = args[0];
        final String streamList = args[1];

        DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                .clientId(ClientId.apply("console-proxy-writer"))
                .name("console-proxy-writer")
                .thriftmux(true)
                .finagleNameStr(finagleNameStr)
                .build();
        String[] streamNameList = StringUtils.split(streamList, ',');
        RRMultiWriter<Integer, String> writer = new RRMultiWriter(streamNameList, client);

        ConsoleReader reader = new ConsoleReader();
        String line;
        while ((line = reader.readLine(PROMPT_MESSAGE)) != null) {
            writer.write(line)
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

        client.close();
    }

}
