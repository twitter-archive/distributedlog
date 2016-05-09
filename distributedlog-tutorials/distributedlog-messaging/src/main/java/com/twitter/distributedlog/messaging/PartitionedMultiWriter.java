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
import com.twitter.util.Future;

import java.nio.ByteBuffer;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Partitioned Writer
 */
public class PartitionedMultiWriter<KEY, VALUE> {

    private final String[] streams;
    private final Partitioner<KEY> partitioner;
    private final DistributedLogClient client;

    public PartitionedMultiWriter(String[] streams,
                                  Partitioner<KEY> partitioner,
                                  DistributedLogClient client) {
        this.streams = streams;
        this.partitioner = partitioner;
        this.client = client;
    }

    public Future<DLSN> write(KEY key, VALUE value) {
        int pid = partitioner.partition(key, streams.length);
        return client.write(streams[pid], ByteBuffer.wrap(value.toString().getBytes(UTF_8)));
    }

}
