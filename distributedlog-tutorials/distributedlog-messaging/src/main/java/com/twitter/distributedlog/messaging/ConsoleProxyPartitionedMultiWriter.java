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
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import jline.ConsoleReader;
import jline.Terminal;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Writer write records from console
 */
public class ConsoleProxyPartitionedMultiWriter {

    private final static String HELP = "ConsoleProxyPartitionedMultiWriter <finagle-name> <stream-1>[,<stream-2>,...,<stream-n>]";
    private final static String PROMPT_MESSAGE = "[dlog] > ";

    static class PartitionedWriter<KEY, VALUE> {

        private final String[] streams;
        private final Partitioner<KEY> partitioner;
        private final DistributedLogClient client;

        PartitionedWriter(String[] streams,
                          Partitioner<KEY> partitioner,
                          DistributedLogClient client) {
            this.streams = streams;
            this.partitioner = partitioner;
            this.client = client;
        }

        Future<DLSN> write(KEY key, VALUE value) {
            int pid = partitioner.partition(key, streams.length);
            return client.write(streams[pid], ByteBuffer.wrap(value.toString().getBytes(UTF_8)));
        }

    }

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.out.println(HELP);
            return;
        }

        String finagleNameStr = args[0];
        final String streamList = args[1];

        DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                .clientId(ClientId$.MODULE$.apply("console-proxy-writer"))
                .name("console-proxy-writer")
                .thriftmux(true)
                .finagleNameStr(finagleNameStr)
                .build();
        String[] streamNameList = StringUtils.split(streamList, ',');
        PartitionedWriter<Integer, String> partitionedWriter =
                new PartitionedWriter<Integer, String>(
                        streamNameList,
                        new IntPartitioner(),
                        client);

        ConsoleReader reader = new ConsoleReader();
        String line;
        while ((line = reader.readLine(PROMPT_MESSAGE)) != null) {
            String[] parts = StringUtils.split(line, ':');
            if (parts.length != 2) {
                System.out.println("Invalid input. Needs 'KEY:VALUE'");
                continue;
            }
            int key;
            try {
                key = Integer.parseInt(parts[0]);
            } catch (NumberFormatException nfe) {
                System.out.println("Invalid input. Needs 'KEY:VALUE'");
                continue;
            }
            String value = parts[1];

            partitionedWriter.write(key, value)
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
