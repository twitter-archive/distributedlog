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
package com.twitter.distributedlog.service.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.exceptions.StreamUnavailableException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.streamset.Partition;
import com.twitter.distributedlog.service.streamset.PartitionMap;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamManagerImpl is the default implementation responsible for creating, destroying, and keeping track
 * of Streams.
 *
 * StreamFactory, supplied to StreamManagerImpl in the constructor below, is reposible simply for creating
 * a stream object in isolation from the rest of the system. We pass a StreamFactory in instead of simply
 * creating StreamImpl's ourselves in order to inject dependencies without bloating the StreamManagerImpl
 * constructor.
 */
public class StreamManagerImpl implements StreamManager {
    static final Logger logger = LoggerFactory.getLogger(StreamManagerImpl.class);

    private final ConcurrentHashMap<String, Stream> streams =
        new ConcurrentHashMap<String, Stream>();
    private final AtomicInteger numCached = new AtomicInteger(0);

    private final ConcurrentHashMap<String, Stream> acquiredStreams =
        new ConcurrentHashMap<String, Stream>();
    private final AtomicInteger numAcquired = new AtomicInteger(0);

    //
    // Partitions
    //
    private final StreamPartitionConverter partitionConverter;
    private final PartitionMap cachedPartitions = new PartitionMap();
    private final PartitionMap acquiredPartitions = new PartitionMap();

    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService executorService;
    private final DistributedLogConfiguration dlConfig;
    private final StreamConfigProvider streamConfigProvider;
    private final String clientId;
    private boolean closed = false;
    private final StreamFactory streamFactory;
    private final DistributedLogNamespace dlNamespace;

    public StreamManagerImpl(String clientId,
                             DistributedLogConfiguration dlConfig,
                             ScheduledExecutorService executorService,
                             StreamFactory streamFactory,
                             StreamPartitionConverter partitionConverter,
                             StreamConfigProvider streamConfigProvider,
                             DistributedLogNamespace dlNamespace) {
        this.clientId = clientId;
        this.executorService = executorService;
        this.streamFactory = streamFactory;
        this.partitionConverter = partitionConverter;
        this.dlConfig = dlConfig;
        this.streamConfigProvider = streamConfigProvider;
        this.dlNamespace = dlNamespace;
    }

    private DynamicDistributedLogConfiguration getDynConf(String streamName) {
        Optional<DynamicDistributedLogConfiguration> dynDlConf =
                streamConfigProvider.getDynamicStreamConfig(streamName);
        if (dynDlConf.isPresent()) {
            return dynDlConf.get();
        } else {
            return ConfUtils.getConstDynConf(dlConfig);
        }
    }

    @Override
    public boolean allowAcquire(Stream stream) {
        return acquiredPartitions.addPartition(
                stream.getPartition(),
                stream.getStreamConfiguration().getMaxAcquiredPartitionsPerProxy());
    }

    /**
     * Must be enqueued to an executor to avoid deadlocks (close and execute-op both
     * try to acquire the same read-write lock).
     */
    @Override
    public Future<Void> deleteAndRemoveAsync(final String stream) {
        final Promise<Void> result = new Promise<Void>();
        java.util.concurrent.Future<?> scheduleFuture = schedule(new Runnable() {
            @Override
            public void run() {
                result.become(doDeleteAndRemoveAsync(stream));
            }
        }, 0);
        if (null == scheduleFuture) {
            return Future.exception(
                new ServiceUnavailableException("Couldn't schedule a delete task."));
        }
        return result;
    }

    /**
     * Must be enqueued to an executor to avoid deadlocks (close and execute-op both
     * try to acquire the same read-write lock).
     */
    @Override
    public Future<Void> closeAndRemoveAsync(final String streamName) {
        final Promise<Void> releasePromise = new Promise<Void>();
        java.util.concurrent.Future<?> scheduleFuture = schedule(new Runnable() {
            @Override
            public void run() {
                releasePromise.become(doCloseAndRemoveAsync(streamName));
            }
        }, 0);
        if (null == scheduleFuture) {
            return Future.exception(
                new ServiceUnavailableException("Couldn't schedule a release task."));
        }
        return releasePromise;
    }

    @Override
    public Future<Void> createStreamAsync(final String stream) {
        final Promise<Void> createPromise = new Promise<Void>();
        java.util.concurrent.Future<?> scheduleFuture = schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    dlNamespace.createLog(stream);
                    createPromise.setValue(null);
                } catch (Exception e) {
                    createPromise.setException(e);
                }
            }
        }, 0);
        if (null == scheduleFuture) {
            return Future.exception(
                new ServiceUnavailableException("Couldn't schedule a create task."));
        }
        return createPromise;
    }

    @Override
    public void notifyReleased(Stream stream) {
        acquiredPartitions.removePartition(stream.getPartition());
        if (acquiredStreams.remove(stream.getStreamName(), stream)) {
            numAcquired.getAndDecrement();
        }
    }

    @Override
    public void notifyAcquired(Stream stream) {
        if (null == acquiredStreams.put(stream.getStreamName(), stream)) {
            numAcquired.getAndIncrement();
        }
    }

    @Override
    public boolean notifyRemoved(Stream stream) {
        cachedPartitions.removePartition(stream.getPartition());
        if (streams.remove(stream.getStreamName(), stream)) {
            numCached.getAndDecrement();
            return true;
        }
        return false;
    }

    @Override
    public Map<String, String> getStreamOwnershipMap(Optional<String> regex) {
        Map<String, String> ownershipMap = new HashMap<String, String>();
        for (String name : acquiredStreams.keySet()) {
            if (regex.isPresent() && !name.matches(regex.get())) {
                continue;
            }
            Stream stream = acquiredStreams.get(name);
            if (null == stream) {
                continue;
            }
            String owner = stream.getOwner();
            if (null == owner) {
                ownershipMap.put(name, clientId);
            }
        }
        return ownershipMap;
    }

    @Override
    public Stream getStream(String stream) {
        return streams.get(stream);
    }

    @Override
    public Stream getOrCreateStream(String streamName) throws IOException {
        Stream stream = streams.get(streamName);
        if (null == stream) {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return null;
                }
                DynamicDistributedLogConfiguration dynConf = getDynConf(streamName);
                int maxCachedPartitions = dynConf.getMaxCachedPartitionsPerProxy();

                // get partition from the stream name
                Partition partition = partitionConverter.convert(streamName);

                // add partition to cached map
                if (!cachedPartitions.addPartition(partition, maxCachedPartitions)) {
                    throw new StreamUnavailableException("Stream " + stream
                            + " is not allowed to cache more than " + maxCachedPartitions + " partitions");
                }

                stream = newStream(streamName, dynConf);
                Stream oldWriter = streams.putIfAbsent(streamName, stream);
                if (null != oldWriter) {
                    stream = oldWriter;
                } else {
                    numCached.getAndIncrement();
                    logger.info("Inserted mapping stream name {} -> stream {}", streamName, stream);
                    stream.initialize();
                    stream.start();
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return stream;
    }

    @Override
    public Future<List<Void>> closeStreams() {
        int numAcquired = acquiredStreams.size();
        int numCached = streams.size();
        logger.info("Closing all acquired streams : acquired = {}, cached = {}.",
            numAcquired, numCached);
        Set<Stream> streamsToClose = new HashSet<Stream>();
        streamsToClose.addAll(streams.values());
        return closeStreams(streamsToClose, Optional.<RateLimiter>absent());
    }

    @Override
    public void scheduleRemoval(final Stream stream, long delayMs) {
        logger.info("Scheduling removal of stream {} from cache after {} sec.",
            stream.getStreamName(), delayMs);
        schedule(new Runnable() {
            @Override
            public void run() {
                if (notifyRemoved(stream)) {
                    logger.info("Removed cached stream {} after probation.", stream.getStreamName());
                } else {
                    logger.info("Cached stream {} already removed.", stream.getStreamName());
                }
            }
        }, delayMs);
    }

    @Override
    public int numAcquired() {
        return numAcquired.get();
    }

    @Override
    public int numCached() {
        return numCached.get();
    }

    @Override
    public boolean isAcquired(String streamName) {
        return acquiredStreams.containsKey(streamName);
    }

    @Override
    public void close() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    private Future<List<Void>> closeStreams(Set<Stream> streamsToClose, Optional<RateLimiter> rateLimiter) {
        if (streamsToClose.isEmpty()) {
            logger.info("No streams to close.");
            List<Void> emptyList = new ArrayList<Void>();
            return Future.value(emptyList);
        }
        List<Future<Void>> futures = new ArrayList<Future<Void>>(streamsToClose.size());
        for (Stream stream : streamsToClose) {
            if (rateLimiter.isPresent()) {
                rateLimiter.get().acquire();
            }
            futures.add(stream.requestClose("Close Streams"));
        }
        return Future.collect(futures);
    }

    private Stream newStream(String name, DynamicDistributedLogConfiguration streamConf) {
        return streamFactory.create(name, streamConf, this);
    }

    public Future<Void> doCloseAndRemoveAsync(final String streamName) {
        Stream stream = streams.get(streamName);
        if (null == stream) {
            logger.info("No stream {} to release.", streamName);
            return Future.value(null);
        } else {
            return stream.requestClose("release ownership");
        }
    }

    /**
     * Dont schedule if we're closed - closeLock is acquired to close, so if we acquire the
     * lock and discover we're not closed, we won't schedule.
     */
    private java.util.concurrent.Future<?> schedule(Runnable runnable, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return null;
            } else if (delayMs > 0) {
                return executorService.schedule(runnable, delayMs, TimeUnit.MILLISECONDS);
            } else {
                return executorService.submit(runnable);
            }
        } catch (RejectedExecutionException ree) {
            logger.error("Failed to schedule task {} in {} ms : ",
                    new Object[] { runnable, delayMs, ree });
            return null;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private Future<Void> doDeleteAndRemoveAsync(final String streamName) {
        Stream stream = streams.get(streamName);
        if (null == stream) {
            logger.warn("No stream {} to delete.", streamName);
            return Future.exception(new UnexpectedException("No stream " + streamName + " to delete."));
        } else {
            Future<Void> result;
            logger.info("Deleting stream {}, {}", streamName, stream);
            try {
                stream.delete();
                result = stream.requestClose("Stream Deleted");
            } catch (IOException e) {
                logger.error("Failed on removing stream {} : ", streamName, e);
                result = Future.exception(e);
            }
            return result;
        }
    }

    @VisibleForTesting
    public ConcurrentHashMap<String, Stream> getCachedStreams() {
        return streams;
    }

    @VisibleForTesting
    public ConcurrentHashMap<String, Stream> getAcquiredStreams() {
        return acquiredStreams;
    }
}
