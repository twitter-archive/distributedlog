package com.twitter.distributedlog.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.messaging.PartitionedMultiWriter;
import com.twitter.distributedlog.messaging.Partitioner;
import com.twitter.distributedlog.messaging.RRMultiWriter;
import com.twitter.distributedlog.service.DistributedLogClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * It is a kafka producer that uses dl streams
 */
public class KafkaDistributedLogProducer<K, V> implements Producer<K, V>, Partitioner<K> {

    private final DistributedLogClient client;
    private final int numPartitions;
    private final ConcurrentMap<String, PartitionedMultiWriter<K, V>> partitionedWriters;
    private final ConcurrentMap<String, RRMultiWriter<K, V>> unpartitionedWriters;

    // Assume all streams have same partitions
    public KafkaDistributedLogProducer(DistributedLogClient client,
                                       int numPartitions) {
        this.client = client;
        this.numPartitions = numPartitions;
        this.partitionedWriters = new ConcurrentHashMap<String, PartitionedMultiWriter<K, V>>();
        this.unpartitionedWriters = new ConcurrentHashMap<String, RRMultiWriter<K, V>>();
    }

    @Override
    public int partition(K k, int totalPartitions) {
        if (null != k) {
            return k.hashCode() % totalPartitions;
        }
        return -1;
    }

    private String[] getStreamsForTopic(String topic) {
        String[] streams = new String[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            streams[i] = String.format("%s-%d", topic, i);
        }
        return streams;
    }

    private PartitionedMultiWriter<K, V> getPartitionedMultiWriter(String topic) {
        PartitionedMultiWriter<K, V> writer = partitionedWriters.get(topic);
        if (null == writer) {
            PartitionedMultiWriter<K, V> newWriter = new PartitionedMultiWriter<K, V>(
                    getStreamsForTopic(topic), this, client);
            PartitionedMultiWriter<K, V> oldWriter = partitionedWriters.putIfAbsent(topic, newWriter);
            if (null != oldWriter) {
                writer = oldWriter;
            } else {
                writer = newWriter;
            }
        }
        return writer;
    }

    private RRMultiWriter<K, V> getUnpartitionedMultiWriter(String topic) {
        RRMultiWriter<K, V> writer = unpartitionedWriters.get(topic);
        if (null == writer) {
            RRMultiWriter<K, V> newWriter = new RRMultiWriter<K, V>(
                    getStreamsForTopic(topic), client);
            RRMultiWriter<K, V> oldWriter = unpartitionedWriters.putIfAbsent(topic, newWriter);
            if (null != oldWriter) {
                writer = oldWriter;
            } else {
                writer = newWriter;
            }
        }
        return writer;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        return send(producerRecord, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        com.twitter.util.Future<DLSN> dlsnFuture;
        if (null == producerRecord.key()) {
            dlsnFuture = getUnpartitionedMultiWriter(producerRecord.topic()).write(producerRecord.value());
        } else {
            // TODO: be able to publish to a specific partition
            dlsnFuture = getPartitionedMultiWriter(producerRecord.topic()).write(producerRecord.key(),
                    producerRecord.value());
        }
        return new DLFutureRecordMetadata(producerRecord.topic(), dlsnFuture, callback);
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        String[] streams = getStreamsForTopic(s);
        List<PartitionInfo> partitions = Lists.newArrayListWithExpectedSize(streams.length);
        for (int i = 0; i < streams.length; i++) {
            // TODO: maybe add getOwner from dl write proxy to return the owner of the partition
            partitions.add(new PartitionInfo(s, i, null, null, null));
        }
        return partitions;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // no-op
        return Maps.newHashMap();
    }

    @Override
    public void close() {
        partitionedWriters.clear();
        unpartitionedWriters.clear();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        partitionedWriters.clear();
        unpartitionedWriters.clear();
    }
}
