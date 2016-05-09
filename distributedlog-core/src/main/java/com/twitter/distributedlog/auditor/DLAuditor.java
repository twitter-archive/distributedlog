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
package com.twitter.distributedlog.auditor;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.twitter.distributedlog.BKDistributedLogNamespace;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.DLUtils;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAccessor;
import org.apache.bookkeeper.client.LedgerHandle;
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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Charsets.UTF_8;

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

    private BookKeeperClient getBookKeeperClient(com.twitter.distributedlog.DistributedLogManagerFactory factory) {
        DistributedLogNamespace namespace = factory.getNamespace();
        assert(namespace instanceof BKDistributedLogNamespace);
        return ((BKDistributedLogNamespace) namespace).getReaderBKC();
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
            logger.info("Resolved bookkeeper config : {}", bkdlConfig);

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

        executeAction(poolQueue, 10, new Action<String>() {
            @Override
            public void execute(String poolPath) throws IOException {
                try {
                    collectLedgersFromPool(poolPath);
                } catch (InterruptedException e) {
                    throw new DLInterruptedException("Interrupted on collecting ledgers from allocation pool " + poolPath, e);
                } catch (KeeperException e) {
                    throw new ZKException("Failed to collect ledgers from allocation pool " + poolPath, e.code());
                }
            }

            private void collectLedgersFromPool(String poolPath)
                    throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException, KeeperException {
                List<String> allocators = getZooKeeperClient(factory).get()
                                        .getChildren(poolPath, false);
                for (String allocator : allocators) {
                    String allocatorPath = poolPath + "/" + allocator;
                    byte[] data = getZooKeeperClient(factory).get().getData(allocatorPath, false, new Stat());
                    if (null != data && data.length > 0) {
                        try {
                            long ledgerId = DLUtils.bytes2LedgerId(data);
                            synchronized (ledgers) {
                                ledgers.add(ledgerId);
                            }
                        } catch (NumberFormatException nfe) {
                            logger.warn("Invalid ledger found in allocator path {} : ", allocatorPath, nfe);
                        }
                    }
                }
            }
        });

        logger.info("Collected ledgers from allocators for {}.", uri);
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

        executeAction(streamQueue, 10, new Action<String>() {
            @Override
            public void execute(String stream) throws IOException {
                collectLedgersFromStream(factory, stream, ledgers);
            }
        });
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

    /**
     * Calculating stream space usage from given <i>uri</i>.
     *
     * @param uri dl uri
     * @throws IOException
     */
    public Map<String, Long> calculateStreamSpaceUsage(final URI uri) throws IOException {
        logger.info("Collecting stream space usage for {}.", uri);
        com.twitter.distributedlog.DistributedLogManagerFactory factory =
                new com.twitter.distributedlog.DistributedLogManagerFactory(conf, uri);
        try {
            return calculateStreamSpaceUsage(uri, factory);
        } finally {
            factory.close();
        }
    }

    private Map<String, Long> calculateStreamSpaceUsage(
            final URI uri, final com.twitter.distributedlog.DistributedLogManagerFactory factory)
        throws IOException {
        Collection<String> streams = factory.enumerateAllLogsInNamespace();
        final LinkedBlockingQueue<String> streamQueue = new LinkedBlockingQueue<String>();
        streamQueue.addAll(streams);

        final Map<String, Long> streamSpaceUsageMap =
                new ConcurrentSkipListMap<String, Long>();
        final AtomicInteger numStreamsCollected = new AtomicInteger(0);

        executeAction(streamQueue, 10, new Action<String>() {
            @Override
            public void execute(String stream) throws IOException {
                streamSpaceUsageMap.put(stream,
                        calculateStreamSpaceUsage(factory, stream));
                if (numStreamsCollected.incrementAndGet() % 1000 == 0) {
                    logger.info("Calculated {} streams from uri {}.", numStreamsCollected.get(), uri);
                }
            }
        });

        return streamSpaceUsageMap;
    }

    private long calculateStreamSpaceUsage(final com.twitter.distributedlog.DistributedLogManagerFactory factory,
                                           final String stream) throws IOException {
        DistributedLogManager dlm = factory.createDistributedLogManager(stream,
                com.twitter.distributedlog.DistributedLogManagerFactory.ClientSharingOption.SharedClients);
        long totalBytes = 0;
        try {
            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            for (LogSegmentMetadata segment : segments) {
                try {
                    LedgerHandle lh = getBookKeeperClient(factory).get().openLedgerNoRecovery(segment.getLedgerId(),
                            BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
                    totalBytes += lh.getLength();
                    lh.close();
                } catch (BKException e) {
                    logger.error("Failed to open ledger {} : ", segment.getLedgerId(), e);
                    throw new IOException("Failed to open ledger " + segment.getLedgerId(), e);
                } catch (InterruptedException e) {
                    logger.warn("Interrupted on opening ledger {} : ", segment.getLedgerId(), e);
                    Thread.currentThread().interrupt();
                    throw new DLInterruptedException("Interrupted on opening ledger " + segment.getLedgerId(), e);
                }
            }
        } finally {
            dlm.close();
        }
        return totalBytes;
    }

    public long calculateLedgerSpaceUsage(URI uri) throws IOException {
        List<URI> uris = Lists.newArrayList(uri);
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
            logger.info("Resolved bookkeeper config : {}", bkdlConfig);

            BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                    .name("DLAuditor-BK")
                    .dlConfig(conf)
                    .zkServers(bkdlConfig.getBkZkServersForWriter())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .build();
            try {
                return calculateLedgerSpaceUsage(bkc, executorService);
            } finally {
                bkc.close();
            }
        } finally {
            zkc.close();
            executorService.shutdown();
        }
    }

    private long calculateLedgerSpaceUsage(BookKeeperClient bkc,
                                           final ExecutorService executorService)
        throws IOException {
        final AtomicLong totalBytes = new AtomicLong(0);
        final AtomicLong totalEntries = new AtomicLong(0);
        final AtomicLong numLedgers = new AtomicLong(0);

        LedgerManager lm = BookKeeperAccessor.getLedgerManager(bkc.get());

        final SettableFuture<Void> doneFuture = SettableFuture.create();
        final BookKeeper bk = bkc.get();

        BookkeeperInternalCallbacks.Processor<Long> collector =
                new BookkeeperInternalCallbacks.Processor<Long>() {
            @Override
            public void process(final Long lid,
                                final AsyncCallback.VoidCallback cb) {
                numLedgers.incrementAndGet();
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        bk.asyncOpenLedgerNoRecovery(lid, BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8),
                                new org.apache.bookkeeper.client.AsyncCallback.OpenCallback() {
                            @Override
                            public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                                final int cbRc;
                                if (BKException.Code.OK == rc) {
                                    totalBytes.addAndGet(lh.getLength());
                                    totalEntries.addAndGet(lh.getLastAddConfirmed() + 1);
                                    cbRc = rc;
                                } else {
                                    cbRc = BKException.Code.ZKException;
                                }
                                executorService.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        cb.processResult(cbRc, null, null);
                                    }
                                });
                            }
                        }, null);
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
        lm.asyncProcessLedgers(collector, finalCb, null, BKException.Code.OK, BKException.Code.ZKException);
        try {
            doneFuture.get();
            logger.info("calculated {} ledgers\n\ttotal bytes = {}\n\ttotal entries = {}",
                    new Object[] { numLedgers.get(), totalBytes.get(), totalEntries.get() });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on calculating ledger space : ", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)(e.getCause());
            } else {
                throw new IOException("Failed to calculate ledger space : ", e.getCause());
            }
        }
        return totalBytes.get();
    }

    public void close() {
        // no-op
    }

    static interface Action<T> {
        void execute(T item) throws IOException ;
    }

    static <T> void executeAction(final LinkedBlockingQueue<T> queue,
                                  final int numThreads,
                                  final Action<T> action) throws IOException {
        final CountDownLatch failureLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(queue.size());
        final AtomicInteger numFailures = new AtomicInteger(0);
        final AtomicInteger completedThreads = new AtomicInteger(0);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            for (int i = 0 ; i < numThreads; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            T item = queue.poll();
                            if (null == item) {
                                break;
                            }
                            try {
                                action.execute(item);
                            } catch (IOException ioe) {
                                logger.error("Failed to execute action on item '{}'", item, ioe);
                                numFailures.incrementAndGet();
                                failureLatch.countDown();
                                break;
                            }
                            doneLatch.countDown();
                        }
                        if (numFailures.get() == 0 && completedThreads.incrementAndGet() == numThreads) {
                            failureLatch.countDown();
                        }
                    }
                });
            }
            try {
                failureLatch.await();
                if (numFailures.get() > 0) {
                    throw new IOException("Encountered " + numFailures.get() + " failures on executing action.");
                }
                doneLatch.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted on executing action", ie);
                throw new DLInterruptedException("Interrupted on executing action", ie);
            }
        } finally {
            executorService.shutdown();
        }
    }

}
