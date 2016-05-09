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
