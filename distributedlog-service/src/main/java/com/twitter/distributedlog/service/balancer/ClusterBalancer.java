package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.service.ClientUtils;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A balancer balances ownerships with a cluster of targets
 */
public class ClusterBalancer implements Balancer {

    static final Logger logger = LoggerFactory.getLogger(ClusterBalancer.class);

    /**
     * Represent a single host. Ordered by number of streams in desc order.
     */
    static class Host {

        final SocketAddress address;
        final Set<String> streams;
        final DistributedLogClientBuilder clientBuilder;
        DistributedLogClient client = null;
        MonitorServiceClient monitor = null;

        Host(SocketAddress address, Set<String> streams,
             DistributedLogClientBuilder clientBuilder) {
            this.address = address;
            this.streams = streams;
            this.clientBuilder = clientBuilder;
        }

        private void initializeClientsIfNeeded() {
            if (null == client) {
                Pair<DistributedLogClient, MonitorServiceClient> clientPair =
                        createDistributedLogClient(address, clientBuilder);
                client = clientPair.getLeft();
                monitor = clientPair.getRight();
            }
        }

        synchronized DistributedLogClient getClient() {
            initializeClientsIfNeeded();
            return client;
        }

        synchronized MonitorServiceClient getMonitor() {
            initializeClientsIfNeeded();
            return monitor;
        }

        synchronized void close() {
            if (null != client) {
                client.close();
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Host(").append(address).append(")");
            return sb.toString();
        }
    }

    static class HostComparator implements Comparator<Host>, Serializable {
        private static final long serialVersionUID = 7984973796525102538L;

        @Override
        public int compare(Host h1, Host h2) {
            return h2.streams.size() - h1.streams.size();
        }
    }

    protected final DistributedLogClientBuilder clientBuilder;
    protected final DistributedLogClient client;
    protected final MonitorServiceClient monitor;

    public ClusterBalancer(DistributedLogClientBuilder clientBuilder) {
        this(clientBuilder, ClientUtils.buildClient(clientBuilder));
    }

    ClusterBalancer(DistributedLogClientBuilder clientBuilder,
                    Pair<DistributedLogClient, MonitorServiceClient> clientPair) {
        this.clientBuilder = clientBuilder;
        this.client = clientPair.getLeft();
        this.monitor = clientPair.getRight();
    }

    /**
     * Build a new distributedlog client to a single host <i>host</i>.
     *
     * @param host
     *          host to access
     * @return distributedlog clients
     */
    static Pair<DistributedLogClient, MonitorServiceClient> createDistributedLogClient(
            SocketAddress host, DistributedLogClientBuilder clientBuilder) {
        DistributedLogClientBuilder newBuilder =
                DistributedLogClientBuilder.newBuilder(clientBuilder).host(host);
        return ClientUtils.buildClient(newBuilder);
    }

    @Override
    public void balanceAll(String source,
                           int rebalanceConcurrency, /* unused */
                           Optional<RateLimiter> rebalanceRateLimiter) {
        balance(0, 0.0f, rebalanceConcurrency, Optional.of(source), rebalanceRateLimiter);
    }

    @Override
    public void balance(int rebalanceWaterMark,
                        double rebalanceTolerancePercentage,
                        int rebalanceConcurrency, /* unused */
                        Optional<RateLimiter> rebalanceRateLimiter) {
        Optional<String> source = Optional.absent();
        balance(rebalanceWaterMark, rebalanceTolerancePercentage, rebalanceConcurrency, source, rebalanceRateLimiter);
    }

    public void balance(int rebalanceWaterMark,
                        double rebalanceTolerancePercentage,
                        int rebalanceConcurrency,
                        Optional<String> source,
                        Optional<RateLimiter> rebalanceRateLimiter) {
        Map<SocketAddress, Set<String>> distribution = monitor.getStreamOwnershipDistribution();
        if (distribution.size() <= 1) {
            return;
        }
        SocketAddress sourceAddr = null;
        if (source.isPresent()) {
            sourceAddr = DLSocketAddress.parseSocketAddress(source.get());
            logger.info("Balancer source is {}", sourceAddr);
            if (!distribution.containsKey(sourceAddr)) {
                return;
            }
        }
        // Get the list of hosts ordered by number of streams in DESC order
        List<Host> hosts = new ArrayList<Host>(distribution.size());
        for (Map.Entry<SocketAddress, Set<String>> entry : distribution.entrySet()) {
            Host host = new Host(entry.getKey(), entry.getValue(), clientBuilder);
            hosts.add(host);
        }
        Collections.sort(hosts, new HostComparator());
        try {

            // find the host to move streams from.
            int hostIdxMoveFrom = -1;
            if (null != sourceAddr) {
                for (Host host : hosts) {
                    ++hostIdxMoveFrom;
                    if (sourceAddr.equals(host.address)) {
                        break;
                    }
                }
            }

            // compute the average load.
            int totalStream = 0;
            for (Host host : hosts) {
                totalStream += host.streams.size();
            }
            double averageLoad;
            if (hostIdxMoveFrom >= 0) {
                averageLoad = ((double) totalStream / (hosts.size() - 1));
            } else {
                averageLoad = ((double) totalStream / hosts.size());
            }

            int moveFromLowWaterMark;
            int moveToHighWaterMark = Math.max(1, (int) (averageLoad + averageLoad * rebalanceTolerancePercentage / 100.0f));

            if (hostIdxMoveFrom >= 0) {
                moveFromLowWaterMark = Math.max(0, rebalanceWaterMark);
                moveStreams(
                        hosts,
                        new AtomicInteger(hostIdxMoveFrom), moveFromLowWaterMark,
                        new AtomicInteger(hosts.size() - 1), moveToHighWaterMark,
                        rebalanceRateLimiter);
                moveRemainingStreamsFromSource(hosts.get(hostIdxMoveFrom), hosts, rebalanceRateLimiter);
            } else {
                moveFromLowWaterMark = Math.max((int) Math.ceil(averageLoad), rebalanceWaterMark);
                AtomicInteger moveFrom = new AtomicInteger(0);
                AtomicInteger moveTo = new AtomicInteger(hosts.size() - 1);
                while (moveFrom.get() < moveTo.get()) {
                    moveStreams(hosts, moveFrom, moveFromLowWaterMark, moveTo, moveToHighWaterMark, rebalanceRateLimiter);
                    moveFrom.incrementAndGet();
                }
            }
        } finally {
            for (Host host : hosts) {
                host.close();
            }
        }
    }

    void moveStreams(List<Host> hosts,
                     AtomicInteger hostIdxMoveFrom,
                     int moveFromLowWaterMark,
                     AtomicInteger hostIdxMoveTo,
                     int moveToHighWaterMark,
                     Optional<RateLimiter> rateLimiter) {
        if (hostIdxMoveFrom.get() < 0 || hostIdxMoveFrom.get() >= hosts.size()
                || hostIdxMoveTo.get() < 0 || hostIdxMoveTo.get() >= hosts.size()
                || hostIdxMoveFrom.get() >= hostIdxMoveTo.get()) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Moving streams : hosts = {}, from = {}, to = {} : from_low_water_mark = {}, to_high_water_mark = {}",
                         new Object[] { hosts, hostIdxMoveFrom.get(), hostIdxMoveTo.get(), moveFromLowWaterMark, moveToHighWaterMark });
        }

        Host hostMoveFrom = hosts.get(hostIdxMoveFrom.get());
        int numStreamsOnFromHost = hostMoveFrom.streams.size();
        if (numStreamsOnFromHost <= moveFromLowWaterMark) {
            // do nothing
            return;
        }

        int numStreamsToMove = numStreamsOnFromHost - moveFromLowWaterMark;
        LinkedList<String> streamsToMove = new LinkedList<String>(hostMoveFrom.streams);
        Collections.shuffle(streamsToMove);

        if (logger.isDebugEnabled()) {
            logger.debug("Try to move {} streams from host {} : streams = {}",
                         new Object[] { numStreamsToMove, hostMoveFrom.address, streamsToMove });
        }

        while (numStreamsToMove-- > 0 && !streamsToMove.isEmpty()) {
            if (rateLimiter.isPresent()) {
                rateLimiter.get().acquire();
            }

            // pick a host to move
            Host hostMoveTo = hosts.get(hostIdxMoveTo.get());
            while (hostMoveTo.streams.size() >= moveToHighWaterMark) {
                int hostIdx = hostIdxMoveTo.decrementAndGet();
                logger.info("move to host : {}, from {}", hostIdx, hostIdxMoveFrom.get());
                if (hostIdx <= hostIdxMoveFrom.get()) {
                    return;
                } else {
                    hostMoveTo = hosts.get(hostIdx);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Target host to move moved to host {} @ {}",
                                hostIdx, hostMoveTo);
                    }
                }
            }

            // pick a stream
            String stream = streamsToMove.remove();

            // move the stream
            if (moveStream(stream, hostMoveFrom, hostMoveTo)) {
                hostMoveFrom.streams.remove(stream);
                hostMoveTo.streams.add(stream);
            }
        }

    }

    void moveRemainingStreamsFromSource(Host source,
                                        List<Host> hosts,
                                        Optional<RateLimiter> rateLimiter) {
        LinkedList<String> streamsToMove = new LinkedList<String>(source.streams);
        Collections.shuffle(streamsToMove);

        if (logger.isDebugEnabled()) {
            logger.debug("Try to move remaining streams from {} : {}", source, streamsToMove);
        }

        int hostIdx = hosts.size() - 1;

        while (!streamsToMove.isEmpty()) {
            if (rateLimiter.isPresent()) {
                rateLimiter.get().acquire();
            }

            Host target = hosts.get(hostIdx);
            if (!target.address.equals(source.address)) {
                String stream = streamsToMove.remove();
                // move the stream
                if (moveStream(stream, source, target)) {
                    source.streams.remove(stream);
                    target.streams.add(stream);
                }
            }
            --hostIdx;
            if (hostIdx < 0) {
                hostIdx = hosts.size() - 1;
            }
        }
    }

    private boolean moveStream(String stream, Host from, Host to) {
        try {
            doMoveStream(stream, from, to);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void doMoveStream(final String stream, final Host from, final Host to) throws Exception {
        logger.info("Moving stream {} from {} to {}.",
                    new Object[] { stream, from.address, to.address });
        Await.result(from.getClient().release(stream).flatMap(new Function<Void, Future<Void>>() {
            @Override
            public Future<Void> apply(Void result) {
                logger.info("Released stream {} from {}.", stream, from.address);
                return to.getMonitor().check(stream).addEventListener(new FutureEventListener<Void>() {

                    @Override
                    public void onSuccess(Void value) {
                        logger.info("Moved stream {} from {} to {}.",
                                    new Object[] { stream, from.address, to.address });
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        logger.info("Failed to move stream {} from {} to {} : ",
                                    new Object[] { stream, from.address, to.address, cause });
                    }
                });
            }
        }));
    }

    @Override
    public void close() {
        client.close();
    }
}
