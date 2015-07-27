package com.twitter.distributedlog.auditor;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import com.twitter.distributedlog.BKDistributedLogNamespace;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.Utils;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.DLUtils;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAccessor;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DL Auditor will audit DL namespace, e.g. find leaked ledger, report disk usage by streams.
 */
@SuppressWarnings("deprecation")
public class DLAuditor {

    private static final Logger logger = LoggerFactory.getLogger(DLAuditor.class);

    private final DistributedLogConfiguration conf;

    public DLAuditor(DistributedLogConfiguration conf) {
        this.conf = conf;
    }

    private ZooKeeperClient getZooKeeperClient(com.twitter.distributedlog.DistributedLogManagerFactory factory) {
        DistributedLogNamespace namespace = factory.getNamespace();
        assert(namespace instanceof BKDistributedLogNamespace);
        return ((BKDistributedLogNamespace) namespace).getSharedWriterZKCForDL();
    }

    private String validateAndGetZKServers(List<URI> uris) {
        URI firstURI = uris.get(0);
        String zkServers = DLUtils.getZKServersFromDLUri(firstURI);
        for (URI uri : uris) {
            if (!zkServers.equalsIgnoreCase(DLUtils.getZKServersFromDLUri(uri))) {
                throw new IllegalArgumentException("Uris don't belong to same zookeeper cluster");
            }
        }
        return zkServers;
    }

    private BKDLConfig resolveBKDLConfig(ZooKeeperClient zkc, List<URI> uris) throws IOException {
        URI firstURI = uris.get(0);
        BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, firstURI);
        for (URI uri : uris) {
            BKDLConfig anotherConfig = BKDLConfig.resolveDLConfig(zkc, uri);
            if (!(Objects.equal(bkdlConfig.getBkLedgersPath(), anotherConfig.getBkLedgersPath())
                    && Objects.equal(bkdlConfig.getBkZkServersForWriter(), anotherConfig.getBkZkServersForWriter()))) {
                throw new IllegalArgumentException("Uris don't use same bookkeeper cluster");
            }
        }
        return bkdlConfig;
    }

    public Pair<Set<Long>, Set<Long>> collectLedgers(List<URI> uris, List<List<String>> allocationPaths)
            throws IOException {
        Preconditions.checkArgument(uris.size() > 0, "No uri provided to audit");

        String zkServers = validateAndGetZKServers(uris);
        RetryPolicy retryPolicy = new BoundExponentialBackoffRetryPolicy(
                conf.getZKRetryBackoffStartMillis(),
                conf.getZKRetryBackoffMaxMillis(),
                Integer.MAX_VALUE);
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .name("DLAuditor-ZK")
                .zkServers(zkServers)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .retryPolicy(retryPolicy)
                .zkAclId(conf.getZkAclId())
                .build();
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            BKDLConfig bkdlConfig = resolveBKDLConfig(zkc, uris);
            logger.info("Resolved bookkeeper config : ", bkdlConfig);

            BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                    .name("DLAuditor-BK")
                    .dlConfig(conf)
                    .zkServers(bkdlConfig.getBkZkServersForWriter())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .build();
            try {
                Set<Long> bkLedgers = collectLedgersFromBK(bkc, executorService);
                Set<Long> dlLedgers = collectLedgersFromDL(uris, allocationPaths);
                return Pair.of(bkLedgers, dlLedgers);
            } finally {
                bkc.close();
            }
        } finally {
            zkc.close();
            executorService.shutdown();
        }
    }

    /**
     * Find leak ledgers phase 1: collect ledgers set.
     */
    private Set<Long> collectLedgersFromBK(BookKeeperClient bkc,
                                           final ExecutorService executorService)
            throws IOException {
        LedgerManager lm = BookKeeperAccessor.getLedgerManager(bkc.get());

        final Set<Long> ledgers = new HashSet<Long>();
        final SettableFuture<Void> doneFuture = SettableFuture.create();

        BookkeeperInternalCallbacks.Processor<Long> collector =
                new BookkeeperInternalCallbacks.Processor<Long>() {
            @Override
            public void process(Long lid,
                                final AsyncCallback.VoidCallback cb) {
                synchronized (ledgers) {
                    ledgers.add(lid);
                    if (0 == ledgers.size() % 1000) {
                        logger.info("Collected {} ledgers", ledgers.size());
                    }
                }
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        cb.processResult(BKException.Code.OK, null, null);
                    }
                });

            }
        };
        AsyncCallback.VoidCallback finalCb = new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (BKException.Code.OK == rc) {
                    doneFuture.set(null);
                } else {
                    doneFuture.setException(BKException.create(rc));
                }
            }
        };
        lm.asyncProcessLedgers(collector, finalCb, null, BKException.Code.OK,
                BKException.Code.ZKException);
        try {
            doneFuture.get();
            logger.info("Collected total {} ledgers", ledgers.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on collecting ledgers : ", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)(e.getCause());
            } else {
                throw new IOException("Failed to collect ledgers : ", e.getCause());
            }
        }
        return ledgers;
    }

    /**
     * Find leak ledgers phase 2: collect ledgers from uris.
     */
    private Set<Long> collectLedgersFromDL(List<URI> uris, List<List<String>> allocationPaths)
            throws IOException {
        final Set<Long> ledgers = new TreeSet<Long>();
        List<com.twitter.distributedlog.DistributedLogManagerFactory> factories =
                new ArrayList<com.twitter.distributedlog.DistributedLogManagerFactory>(uris.size());
        try {
            for (URI uri : uris) {
                factories.add(new com.twitter.distributedlog.DistributedLogManagerFactory(conf, uri));
            }
            final CountDownLatch doneLatch = new CountDownLatch(uris.size());
            final AtomicInteger numFailures = new AtomicInteger(0);
            ExecutorService executor = Executors.newFixedThreadPool(uris.size());
            try {
                int i = 0;
                for (com.twitter.distributedlog.DistributedLogManagerFactory factory : factories) {
                    final com.twitter.distributedlog.DistributedLogManagerFactory dlFactory = factory;
                    final URI uri = uris.get(i);
                    final List<String> aps = allocationPaths.get(i);
                    i++;
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                logger.info("Collecting ledgers from {} : {}", uri, aps);
                                collectLedgersFromAllocator(uri, dlFactory, aps, ledgers);
                                synchronized (ledgers) {
                                    logger.info("Collected {} ledgers from allocators for {} : {} ",
                                            new Object[]{ledgers.size(), uri, ledgers});
                                }
                                collectLedgersFromDL(uri, dlFactory, ledgers);
                            } catch (IOException e) {
                                numFailures.incrementAndGet();
                                logger.info("Error to collect ledgers from DL : ", e);
                            }
                            doneLatch.countDown();
                        }
                    });
                }
                try {
                    doneLatch.await();
                    if (numFailures.get() > 0) {
                        throw new IOException(numFailures.get() + " errors to collect ledgers from DL");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted on collecting ledgers from DL : ", e);
                    throw new DLInterruptedException("Interrupted on collecting ledgers from DL : ", e);
                }
            } finally {
                executor.shutdown();
            }
        } finally {
            for (com.twitter.distributedlog.DistributedLogManagerFactory factory : factories) {
                factory.close();
            }
        }
        return ledgers;
    }

    private void collectLedgersFromAllocator(final URI uri,
                                             final com.twitter.distributedlog.DistributedLogManagerFactory factory,
                                             final List<String> allocationPaths,
                                             final Set<Long> ledgers) throws IOException {
        final LinkedBlockingQueue<String> poolQueue =
                new LinkedBlockingQueue<String>();
        for (String allocationPath : allocationPaths) {
            String rootPath = uri.getPath() + "/" + allocationPath;
            try {
                List<String> pools = getZooKeeperClient(factory).get().getChildren(rootPath, false);
                for (String pool : pools) {
                    poolQueue.add(rootPath + "/" + pool);
                }
            } catch (KeeperException e) {
                throw new ZKException("Failed to get list of pools from " + rootPath, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DLInterruptedException("Interrupted on getting list of pools from " + rootPath, e);
            }
        }
        logger.info("Collecting ledgers from allocators for {} : {}", uri, poolQueue);
        final int numThreads = 10;

        final CountDownLatch failureLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicInteger numFailures = new AtomicInteger(0);

        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            for (int i = 0; i < numThreads; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            final String poolPath = poolQueue.poll();
                            if (null == poolPath) {
                                break;
                            }
                            List<String> allocators;
                            try {
                                allocators = getZooKeeperClient(factory).get()
                                        .getChildren(poolPath, false);
                                for (String allocator : allocators) {
                                    String allocatorPath = poolPath + "/" + allocator;
                                    byte[] data = getZooKeeperClient(factory).get().getData(allocatorPath, false, new Stat());
                                    if (null != data && data.length > 0) {
                                        try {
                                            long ledgerId = Utils.bytes2LedgerId(data);
                                            synchronized (ledgers) {
                                                ledgers.add(ledgerId);
                                            }
                                        } catch (NumberFormatException nfe) {
                                            logger.warn("Invalid ledger found in allocator path {} : ", allocatorPath, nfe);
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("Encountered exception on collecting ledgers from allocator for {}", uri, e);
                                numFailures.incrementAndGet();
                                break;
                            }
                            doneLatch.countDown();
                        }
                        failureLatch.countDown();
                    }
                });
            }
            try {
                failureLatch.await();
                if (numFailures.get() > 0) {
                    throw new IOException("Encountered " + numFailures.get()
                            + " failures on collecting ledgers from DL Allocators");
                }
                doneLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted on collecting ledgers from DL Allocators: ", e);
                throw new DLInterruptedException("Interrupted on collecting ledgers from DL Allocators : ", e);
            }
            logger.info("Collected ledgers from allocators for {}.", uri);
        } finally {
            executorService.shutdown();
        }
    }

    private void collectLedgersFromDL(final URI uri,
                                      final com.twitter.distributedlog.DistributedLogManagerFactory factory,
                                      final Set<Long> ledgers) throws IOException {
        logger.info("Enumerating {} to collect streams.", uri);
        Collection<String> streams = factory.enumerateAllLogsInNamespace();
        final LinkedBlockingQueue<String> streamQueue = new LinkedBlockingQueue<String>();
        streamQueue.addAll(streams);

        logger.info("Collected {} streams from uri {} : {}",
                    new Object[] { streams.size(), uri, streams });

        final CountDownLatch failureLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(streams.size());
        final AtomicInteger numFailures = new AtomicInteger(0);

        int numThreads = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            for (int i = 0; i < numThreads; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        int i = 0;
                        while (true) {
                            String stream = streamQueue.poll();
                            if (null == stream) {
                                break;
                            }
                            try {
                                collectLedgersFromStream(factory, stream, ledgers);
                                i++;
                                if (i % 1000 == 0) {
                                    logger.info("Collected {} streams from uri {}.", i, uri);
                                }
                            } catch (IOException e) {
                                logger.error("Failed to collect ledgers from stream {} : ", stream, e);
                                numFailures.incrementAndGet();
                                break;
                            }
                            doneLatch.countDown();
                        }
                        failureLatch.countDown();
                    }
                });
            }
            try {
                failureLatch.await();
                if (numFailures.get() > 0) {
                    throw new IOException("Encountered " + numFailures.get()
                            + " failures on collecting ledgers from DL");
                }
                doneLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted on collecting ledgers : ", e);
                throw new DLInterruptedException("Interrupted on collecting ledgers : ", e);
            }
        } finally {
            executorService.shutdown();
        }
    }

    private List<Long> collectLedgersFromStream(com.twitter.distributedlog.DistributedLogManagerFactory factory,
                                          String stream,
                                          Set<Long> ledgers)
            throws IOException {
        DistributedLogManager dlm = factory.createDistributedLogManager(stream,
                com.twitter.distributedlog.DistributedLogManagerFactory.ClientSharingOption.SharedClients);
        try {
            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            List<Long> sLedgers = new ArrayList<Long>();
            for (LogSegmentMetadata segment : segments) {
                synchronized (ledgers) {
                    ledgers.add(segment.getLedgerId());
                }
                sLedgers.add(segment.getLedgerId());
            }
            return sLedgers;
        } finally {
            dlm.close();
        }
    }

    public void close() {
        // no-op
    }
}
