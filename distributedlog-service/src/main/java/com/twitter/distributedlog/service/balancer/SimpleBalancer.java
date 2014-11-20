package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.MonitorServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A balancer balances ownerships between two targets.
 */
public class SimpleBalancer implements Balancer {

    static final Logger logger = LoggerFactory.getLogger(SimpleBalancer.class);

    protected final String target1;
    protected final String target2;
    protected final DistributedLogClient targetClient1;
    protected final DistributedLogClient targetClient2;
    protected final MonitorServiceClient targetMonitor1;
    protected final MonitorServiceClient targetMonitor2;

    public SimpleBalancer(String name1,
                          DistributedLogClient client1,
                          MonitorServiceClient monitor1,
                          String name2,
                          DistributedLogClient client2,
                          MonitorServiceClient monitor2) {
        this.target1 = name1;
        this.targetClient1 = client1;
        this.targetMonitor1 = monitor1;
        this.target2 = name2;
        this.targetClient2 = client2;
        this.targetMonitor2 = monitor2;
    }

    protected static int countNumberStreams(Map<SocketAddress, Set<String>> distribution) {
        int count = 0;
        for (Set<String> streams : distribution.values()) {
            count += streams.size();
        }
        return count;
    }

    @Override
    public void balance(int rebalanceWaterMark,
                        double rebalanceTolerancePercentage,
                        int rebalanceConcurrency,
                        Optional<RateLimiter> rebalanceRateLimiter) {
        // get the ownership distributions from individual targets
        Map<SocketAddress, Set<String>> distribution1 = targetMonitor1.getStreamOwnershipDistribution();
        Map<SocketAddress, Set<String>> distribution2 = targetMonitor2.getStreamOwnershipDistribution();

        // get stream counts
        int proxyCount1 = distribution1.size();
        int streamCount1 = countNumberStreams(distribution1);
        int proxyCount2 = distribution2.size();
        int streamCount2 = countNumberStreams(distribution2);

        logger.info("'{}' has {} streams by {} proxies; while '{}' has {} streams by {} proxies.",
                    new Object[] {target1, streamCount1, proxyCount1, target2, streamCount2, proxyCount2 });

        String source, target;
        Map<SocketAddress, Set<String>> srcDistribution, targetDistribution;
        DistributedLogClient srcClient, targetClient;
        MonitorServiceClient srcMonitor, targetMonitor;
        int srcStreamCount, targetStreamCount;
        if (streamCount1 > streamCount2) {
            source = target1;
            srcStreamCount = streamCount1;
            srcClient = targetClient1;
            srcMonitor = targetMonitor1;
            srcDistribution = distribution1;

            target = target2;
            targetStreamCount = streamCount2;
            targetClient = targetClient2;
            targetMonitor = targetMonitor2;
            targetDistribution = distribution2;
        } else {
            source = target2;
            srcStreamCount = streamCount2;
            srcClient = targetClient2;
            srcMonitor = targetMonitor2;
            srcDistribution = distribution2;

            target = target1;
            targetStreamCount = streamCount1;
            targetClient = targetClient1;
            targetMonitor = targetMonitor1;
            targetDistribution = distribution1;
        }

        Map<String, Integer> loadDistribution = new HashMap<String, Integer>();
        loadDistribution.put(source, srcStreamCount);
        loadDistribution.put(target, targetStreamCount);

        // Calculate how many streams to be rebalanced from src region to target region
        int numStreamsToRebalance =
                BalancerUtils.calculateNumStreamsToRebalance(source, loadDistribution, rebalanceWaterMark, rebalanceTolerancePercentage);

        if (numStreamsToRebalance <= 0) {
            logger.info("No streams need to be rebalanced from '{}' to '{}'.", source, target);
            return;
        }

        StreamChooser streamChooser =
                LimitedStreamChooser.of(new CountBasedStreamChooser(srcDistribution), numStreamsToRebalance);
        StreamMover streamMover = new StreamMoverImpl(source, srcClient, srcMonitor, target, targetClient, targetMonitor);

        moveStreams(streamChooser, streamMover, rebalanceConcurrency, rebalanceRateLimiter);
    }

    @Override
    public void balanceAll(String source,
                           int rebalanceConcurrency,
                           Optional<RateLimiter> rebalanceRateLimiter) {
        String target;
        DistributedLogClient sourceClient, targetClient;
        MonitorServiceClient sourceMonitor, targetMonitor;
        if (target1.equals(source)) {
            sourceClient = targetClient1;
            sourceMonitor = targetMonitor1;
            target = target2;
            targetClient = targetClient2;
            targetMonitor = targetMonitor2;
        } else if (target2.equals(source)) {
            sourceClient = targetClient2;
            sourceMonitor = targetMonitor2;
            target = target1;
            targetClient = targetClient1;
            targetMonitor = targetMonitor1;
        } else {
            throw new IllegalArgumentException("Unknown target " + source);
        }

        // get the ownership distributions from individual targets
        Map<SocketAddress, Set<String>> distribution = sourceMonitor.getStreamOwnershipDistribution();

        if (distribution.isEmpty()) {
            return;
        }

        StreamChooser streamChooser = new CountBasedStreamChooser(distribution);
        StreamMover streamMover = new StreamMoverImpl(source, sourceClient, sourceMonitor, target, targetClient, targetMonitor);

        moveStreams(streamChooser, streamMover, rebalanceConcurrency, rebalanceRateLimiter);
    }

    private void moveStreams(StreamChooser streamChooser,
                             StreamMover streamMover,
                             int concurrency,
                             Optional<RateLimiter> rateLimiter) {
        CountDownLatch doneLatch = new CountDownLatch(concurrency);
        RegionMover regionMover = new RegionMover(streamChooser, streamMover, rateLimiter, doneLatch);
        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        try {
            for (int i = 0; i < concurrency; i++) {
                executorService.submit(regionMover);
            }

            try {
                doneLatch.await();
            } catch (InterruptedException e) {
                logger.info("{} is interrupted. Stopping it ...", streamMover);
                regionMover.shutdown();
            }
        } finally {
            executorService.shutdown();
        }

    }

    /**
     * Move streams from <i>src</i> region to <i>target</i> region.
     */
    static class RegionMover implements Runnable {

        final StreamChooser streamChooser;
        final StreamMover streamMover;
        final Optional<RateLimiter> rateLimiter;
        final CountDownLatch doneLatch;
        volatile boolean running = true;

        RegionMover(StreamChooser streamChooser,
                    StreamMover streamMover,
                    Optional<RateLimiter> rateLimiter,
                    CountDownLatch doneLatch) {
            this.streamChooser = streamChooser;
            this.streamMover = streamMover;
            this.rateLimiter = rateLimiter;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            while (running) {
                if (rateLimiter.isPresent()) {
                    rateLimiter.get().acquire();
                }

                String stream = streamChooser.choose();
                if (null == stream) {
                    break;
                }

                streamMover.moveStream(stream);
            }
            doneLatch.countDown();
        }

        void shutdown() {
            running = false;
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
