package com.twitter.distributedlog.messaging;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.Service;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.loadbalancer.Balancers;
import com.twitter.finagle.service.SingletonFactory;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.util.Activity;
import com.twitter.util.Future;
import scala.collection.JavaConversions;

import java.nio.ByteBuffer;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Multi stream writer that leverages finagle load balancer.
 */
public class RRMultiWriter<KEY, VALUE> {

    static class StreamWriter<VALUE> extends Service<VALUE, DLSN> {

        private final String stream;
        private final DistributedLogClient client;

        StreamWriter(String stream,
                     DistributedLogClient client) {
            this.stream = stream;
            this.client = client;
        }

        @Override
        public Future<DLSN> apply(VALUE request) {
            return client.write(stream, ByteBuffer.wrap(request.toString().getBytes(UTF_8)));
        }
    }


    static <VALUE> Set<ServiceFactory<VALUE, DLSN>> initializeServices(
            String[] streams, DistributedLogClient client) {
        Set<ServiceFactory<VALUE, DLSN>> serviceFactories =
                Sets.newHashSet();
        for (String stream : streams) {
            Service<VALUE, DLSN> service = new StreamWriter(stream, client);
            serviceFactories.add(new SingletonFactory<VALUE, DLSN>(service));
        }
        return serviceFactories;
    }

    private final String[] streams;
    private final DistributedLogClient client;
    private final Service<VALUE, DLSN> service;

    public RRMultiWriter(String[] streams,
                         DistributedLogClient client) {
        this.streams = streams;
        this.client = client;
        scala.collection.immutable.Set<ServiceFactory<VALUE, DLSN>> scalaSet =
                JavaConversions.asScalaSet(initializeServices(streams, client)).toSet();
        this.service = Balancers.heap(new scala.util.Random(System.currentTimeMillis()))
                .newBalancer(
                        Activity.value(scalaSet),
                        NullStatsReceiver.get(),
                        new NoBrokersAvailableException("No partitions available")
                ).toService();
    }

    public Future<DLSN> write(VALUE data) {
        return service.apply(data);
    }

}
