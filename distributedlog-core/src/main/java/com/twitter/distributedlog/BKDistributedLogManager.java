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
package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.bk.DynamicQuorumConfigProvider;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.LedgerAllocatorDelegator;
import com.twitter.distributedlog.bk.QuorumConfigProvider;
import com.twitter.distributedlog.bk.SimpleLedgerAllocator;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.AlreadyClosedException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogNotFoundException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.impl.ZKLogSegmentMetadataStore;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForReader;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForWriter;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.lock.SessionLockFactory;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.lock.ZKSessionLockFactory;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.stats.BroadCastStatsLogger;
import com.twitter.distributedlog.stats.ReadAheadExceptionsLogger;
import com.twitter.distributedlog.subscription.SubscriptionStateStore;
import com.twitter.distributedlog.subscription.SubscriptionsStore;
import com.twitter.distributedlog.subscription.ZKSubscriptionStateStore;
import com.twitter.distributedlog.subscription.ZKSubscriptionsStore;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.MonitoredFuturePool;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * <h3>Metrics</h3>
 * <ul>
 * <li> `log_writer/*`: all asynchronous writer related metrics are exposed under scope `log_writer`.
 * See {@link BKAsyncLogWriter} for detail stats.
 * <li> `async_reader/*`: all asyncrhonous reader related metrics are exposed under scope `async_reader`.
 * See {@link BKAsyncLogReaderDLSN} for detail stats.
 * <li> `writer_future_pool/*`: metrics about the future pools that used by writers are exposed under
 * scope `writer_future_pool`. See {@link MonitoredFuturePool} for detail stats.
 * <li> `reader_future_pool/*`: metrics about the future pools that used by readers are exposed under
 * scope `reader_future_pool`. See {@link MonitoredFuturePool} for detail stats.
 * <li> `lock/*`: metrics about the locks used by writers. See {@link DistributedLock} for detail
 * stats.
 * <li> `read_lock/*`: metrics about the locks used by readers. See {@link DistributedLock} for
 * detail stats.
 * <li> `logsegments/*`: metrics about basic operations on log segments. See {@link BKLogHandler} for details.
 * <li> `segments/*`: metrics about write operations on log segments. See {@link BKLogWriteHandler} for details.
 * <li> `readahead_worker/*`: metrics about readahead workers used by readers. See {@link BKLogReadHandler}
 * for details.
 * </ul>
 */
class BKDistributedLogManager extends ZKMetadataAccessor implements DistributedLogManager {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogManager.class);

    static void createLog(DistributedLogConfiguration conf, ZooKeeperClient zkc, URI uri, String streamName)
            throws IOException, InterruptedException {
        Future<ZKLogMetadataForWriter> createFuture = ZKLogMetadataForWriter.of(
                        uri, streamName, conf.getUnpartitionedStreamName(), zkc.get(), zkc.getDefaultACL(), true, true);
        FutureUtils.result(createFuture);
    }

    static final Function<LogRecordWithDLSN, Long> RECORD_2_TXID_FUNCTION =
            new Function<LogRecordWithDLSN, Long>() {
                @Override
                public Long apply(LogRecordWithDLSN record) {
                    return record.getTransactionId();
                }
            };

    static final Function<LogRecordWithDLSN, DLSN> RECORD_2_DLSN_FUNCTION =
            new Function<LogRecordWithDLSN, DLSN>() {
                @Override
                public DLSN apply(LogRecordWithDLSN record) {
                    return record.getDlsn();
                }
            };


    private final String clientId;
    private final int regionId;
    private final String streamIdentifier;
    private final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    private Promise<Void> closePromise;
    private final OrderedScheduler scheduler;
    private final OrderedScheduler readAheadScheduler;
    private boolean ownExecutor;
    private final FeatureProvider featureProvider;
    private final StatsLogger statsLogger;
    private final StatsLogger perLogStatsLogger;
    private final AlertStatsLogger alertStatsLogger;

    // lock factory
    private SessionLockFactory lockFactory = null;

    // log segment metadata stores
    private final LogSegmentMetadataStore writerMetadataStore;
    private final LogSegmentMetadataStore readerMetadataStore;

    // bookkeeper clients
    // NOTE: The actual bookkeeper client is initialized lazily when it is referenced by
    //       {@link com.twitter.distributedlog.BookKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    private final BookKeeperClientBuilder writerBKCBuilder;
    private final BookKeeperClient writerBKC;
    private final boolean ownWriterBKC;
    private final BookKeeperClientBuilder readerBKCBuilder;
    private final BookKeeperClient readerBKC;
    private final boolean ownReaderBKC;

    //
    // Writer Related Variables
    //
    private final LedgerAllocator ledgerAllocator;
    private final PermitLimiter writeLimiter;
    // Log Segment Rolling Manager to control rolling speed
    private final PermitManager logSegmentRollingPermitManager;
    private OrderedScheduler lockStateExecutor = null;

    //
    // Reader Related Variables
    ///
    // read handler for listener.
    private BKLogReadHandler readHandlerForListener = null;
    private FuturePool readerFuturePool = null;
    private final PendingReaders pendingReaders;
    private final ReadAheadExceptionsLogger readAheadExceptionsLogger;

    /**
     * Create a DLM for testing.
     *
     * @param name log name
     * @param conf distributedlog configuration
     * @param uri uri location for the log
     * @param writerZKCBuilder zookeeper builder for writers
     * @param readerZKCBuilder zookeeper builder for readers
     * @param zkcForWriterBKC zookeeper builder for bookkeeper shared by writers
     * @param zkcForReaderBKC zookeeper builder for bookkeeper shared by readers
     * @param writerBKCBuilder bookkeeper builder for writers
     * @param readerBKCBuilder bookkeeper builder for readers
     * @param featureProvider provider to offer features
     * @param writeLimiter write limiter
     * @param statsLogger stats logger to receive stats
     * @throws IOException
     */
    BKDistributedLogManager(String name,
                            DistributedLogConfiguration conf,
                            URI uri,
                            ZooKeeperClientBuilder writerZKCBuilder,
                            ZooKeeperClientBuilder readerZKCBuilder,
                            ZooKeeperClient zkcForWriterBKC,
                            ZooKeeperClient zkcForReaderBKC,
                            BookKeeperClientBuilder writerBKCBuilder,
                            BookKeeperClientBuilder readerBKCBuilder,
                            FeatureProvider featureProvider,
                            PermitLimiter writeLimiter,
                            StatsLogger statsLogger) throws IOException {
        this(name,
             conf,
             ConfUtils.getConstDynConf(conf),
             uri,
             writerZKCBuilder,
             readerZKCBuilder,
             zkcForWriterBKC,
             zkcForReaderBKC,
             writerBKCBuilder,
             readerBKCBuilder,
             null,
             null,
             null,
             OrderedScheduler.newBuilder().name("BKDL-" + name).corePoolSize(1).build(),
             null,
             null,
             null,
             null,
             new ReadAheadExceptionsLogger(statsLogger),
             DistributedLogConstants.UNKNOWN_CLIENT_ID,
             DistributedLogConstants.LOCAL_REGION_ID,
             null,
             writeLimiter,
             PermitManager.UNLIMITED_PERMIT_MANAGER,
             featureProvider,
             statsLogger,
             NullStatsLogger.INSTANCE);
        this.ownExecutor = true;
    }

    /**
     * Create a {@link DistributedLogManager} with supplied resources.
     *
     * @param name log name
     * @param conf distributedlog configuration
     * @param uri uri location for the log
     * @param writerZKCBuilder zookeeper builder for writers
     * @param readerZKCBuilder zookeeper builder for readers
     * @param zkcForWriterBKC zookeeper builder for bookkeeper shared by writers
     * @param zkcForReaderBKC zookeeper builder for bookkeeper shared by readers
     * @param writerBKCBuilder bookkeeper builder for writers
     * @param readerBKCBuilder bookkeeper builder for readers
     * @param lockFactory distributed lock factory
     * @param writerMetadataStore writer metadata store
     * @param readerMetadataStore reader metadata store
     * @param scheduler ordered scheduled used by readers and writers
     * @param readAheadScheduler readAhead scheduler used by readers
     * @param lockStateExecutor ordered scheduled used by locks to execute lock actions
     * @param channelFactory client socket channel factory to build bookkeeper clients
     * @param requestTimer request timer to build bookkeeper clients
     * @param readAheadExceptionsLogger stats logger to record readahead exceptions
     * @param clientId client id that used to initiate the locks
     * @param regionId region id that would be encrypted as part of log segment metadata
     *                 to indicate which region that the log segment will be created
     * @param ledgerAllocator ledger allocator to allocate ledgers
     * @param featureProvider provider to offer features
     * @param writeLimiter write limiter
     * @param statsLogger stats logger to receive stats
     * @param perLogStatsLogger stats logger to receive per log stats
     * @throws IOException
     */
    BKDistributedLogManager(String name,
                            DistributedLogConfiguration conf,
                            DynamicDistributedLogConfiguration dynConf,
                            URI uri,
                            ZooKeeperClientBuilder writerZKCBuilder,
                            ZooKeeperClientBuilder readerZKCBuilder,
                            ZooKeeperClient zkcForWriterBKC,
                            ZooKeeperClient zkcForReaderBKC,
                            BookKeeperClientBuilder writerBKCBuilder,
                            BookKeeperClientBuilder readerBKCBuilder,
                            SessionLockFactory lockFactory,
                            LogSegmentMetadataStore writerMetadataStore,
                            LogSegmentMetadataStore readerMetadataStore,
                            OrderedScheduler scheduler,
                            OrderedScheduler readAheadScheduler,
                            OrderedScheduler lockStateExecutor,
                            ClientSocketChannelFactory channelFactory,
                            HashedWheelTimer requestTimer,
                            ReadAheadExceptionsLogger readAheadExceptionsLogger,
                            String clientId,
                            Integer regionId,
                            LedgerAllocator ledgerAllocator,
                            PermitLimiter writeLimiter,
                            PermitManager logSegmentRollingPermitManager,
                            FeatureProvider featureProvider,
                            StatsLogger statsLogger,
                            StatsLogger perLogStatsLogger) throws IOException {
        super(name, conf, uri, writerZKCBuilder, readerZKCBuilder, statsLogger);
        Preconditions.checkNotNull(readAheadExceptionsLogger, "No ReadAhead Stats Logger Provided.");
        this.conf = conf;
        this.dynConf = dynConf;
        this.scheduler = scheduler;
        this.lockFactory = lockFactory;
        this.lockStateExecutor = lockStateExecutor;
        this.readAheadScheduler = null == readAheadScheduler ? scheduler : readAheadScheduler;
        this.statsLogger = statsLogger;
        this.perLogStatsLogger = BroadCastStatsLogger.masterslave(perLogStatsLogger, statsLogger);
        this.ownExecutor = false;
        this.pendingReaders = new PendingReaders(scheduler);
        this.regionId = regionId;
        this.clientId = clientId;
        this.streamIdentifier = conf.getUnpartitionedStreamName();
        this.ledgerAllocator = ledgerAllocator;
        this.writeLimiter = writeLimiter;
        this.logSegmentRollingPermitManager = logSegmentRollingPermitManager;

        if (null == writerMetadataStore) {
            this.writerMetadataStore = new ZKLogSegmentMetadataStore(conf, writerZKC, scheduler);
        } else {
            this.writerMetadataStore = writerMetadataStore;
        }
        if (null == readerMetadataStore) {
            this.readerMetadataStore = new ZKLogSegmentMetadataStore(conf, readerZKC, scheduler);
        } else {
            this.readerMetadataStore = readerMetadataStore;
        }

        // create the bkc for writers
        if (null == writerBKCBuilder) {
            // resolve uri
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(writerZKC, uri);
            BKDLConfig.propagateConfiguration(bkdlConfig, conf);
            this.writerBKCBuilder = BookKeeperClientBuilder.newBuilder()
                    .dlConfig(conf)
                    .name(String.format("bk:%s:dlm_writer_shared", name))
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .channelFactory(channelFactory)
                    .requestTimer(requestTimer)
                    .statsLogger(statsLogger);
            if (null == zkcForWriterBKC) {
                this.writerBKCBuilder.zkServers(bkdlConfig.getBkZkServersForWriter());
            } else {
                this.writerBKCBuilder.zkc(zkcForWriterBKC);
            }
            this.ownWriterBKC = true;
        } else {
            this.writerBKCBuilder = writerBKCBuilder;
            this.ownWriterBKC = false;
        }
        this.writerBKC = this.writerBKCBuilder.build();

        // create the bkc for readers
        if (null == readerBKCBuilder) {
            // resolve uri
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(writerZKC, uri);
            BKDLConfig.propagateConfiguration(bkdlConfig, conf);
            if (bkdlConfig.getBkZkServersForWriter().equals(bkdlConfig.getBkZkServersForReader())) {
                this.readerBKCBuilder = this.writerBKCBuilder;
                this.ownReaderBKC = false;
            } else {
                this.readerBKCBuilder = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(conf)
                        .name(String.format("bk:%s:dlm_reader_shared", name))
                        .ledgersPath(bkdlConfig.getBkLedgersPath())
                        .channelFactory(channelFactory)
                        .requestTimer(requestTimer)
                        .statsLogger(statsLogger);
                if (null == zkcForReaderBKC) {
                    this.readerBKCBuilder.zkServers(bkdlConfig.getBkZkServersForReader());
                } else {
                    this.readerBKCBuilder.zkc(zkcForReaderBKC);
                }
                this.ownReaderBKC = true;
            }
        } else {
            this.readerBKCBuilder = readerBKCBuilder;
            this.ownReaderBKC = false;
        }
        this.readerBKC = this.readerBKCBuilder.build();

        // Feature Provider
        this.featureProvider = featureProvider;

        // Stats
        this.alertStatsLogger = new AlertStatsLogger(this.perLogStatsLogger, "dl_alert");
        this.readAheadExceptionsLogger = readAheadExceptionsLogger;
    }

    private synchronized OrderedScheduler getLockStateExecutor(boolean createIfNull) {
        if (createIfNull && null == lockStateExecutor && ownExecutor) {
            lockStateExecutor = OrderedScheduler.newBuilder()
                    .corePoolSize(1).name("BKDL-LockState").build();
        }
        return lockStateExecutor;
    }

    private synchronized SessionLockFactory getLockFactory(boolean createIfNull) {
        if (createIfNull && null == lockFactory) {
            lockFactory = new ZKSessionLockFactory(
                    writerZKC,
                    clientId,
                    getLockStateExecutor(createIfNull),
                    conf.getZKNumRetries(),
                    conf.getLockTimeoutMilliSeconds(),
                    conf.getZKRetryBackoffStartMillis(),
                    statsLogger);
        }
        return lockFactory;
    }

    DistributedLogConfiguration getConf() {
        return conf;
    }

    OrderedScheduler getScheduler() {
        return scheduler;
    }

    @VisibleForTesting
    BookKeeperClient getWriterBKC() {
        return this.writerBKC;
    }

    @VisibleForTesting
    BookKeeperClient getReaderBKC() {
        return this.readerBKC;
    }

    @VisibleForTesting
    FuturePool getReaderFuturePool() {
        return this.readerFuturePool;
    }

    @VisibleForTesting
    FeatureProvider getFeatureProvider() {
        return this.featureProvider;
    }

    private synchronized BKLogReadHandler getReadHandlerForListener(boolean create) {
        if (null == readHandlerForListener && create) {
            readHandlerForListener = createReadHandler();
            readHandlerForListener.scheduleGetLedgersTask(true, true);
        }
        return readHandlerForListener;
    }

    @Override
    public List<LogSegmentMetadata> getLogSegments() throws IOException {
        return FutureUtils.result(getLogSegmentsAsync());
    }

    protected Future<List<LogSegmentMetadata>> getLogSegmentsAsync() {
        final BKLogReadHandler readHandler = createReadHandler();
        return readHandler.asyncGetFullLedgerList(true, false).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                readHandler.asyncClose();
                return BoxedUnit.UNIT;
            }
        });
    }

    @Override
    public void registerListener(LogSegmentListener listener) throws IOException {
        BKLogReadHandler readHandler = getReadHandlerForListener(true);
        readHandler.registerListener(listener);
    }

    @Override
    public synchronized void unregisterListener(LogSegmentListener listener) {
        if (null != readHandlerForListener) {
            readHandlerForListener.unregisterListener(listener);
        }
    }

    public void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (null != closePromise) {
            throw new AlreadyClosedException("Executing " + operation + " on already closed DistributedLogManager");
        }

        if (null != writerBKC) {
            writerBKC.checkClosedOrInError();
        }
        if (null != readerBKC) {
            readerBKC.checkClosedOrInError();
        }
    }

    // Create Read Handler

    synchronized BKLogReadHandler createReadHandler() {
        Optional<String> subscriberId = Optional.absent();
        return createReadHandler(subscriberId, false);
    }

    synchronized BKLogReadHandler createReadHandler(Optional<String> subscriberId) {
        return createReadHandler(subscriberId, false);
    }

    synchronized BKLogReadHandler createReadHandler(Optional<String> subscriberId,
                                                    boolean isHandleForReading) {
        return createReadHandler(
                subscriberId,
                getLockStateExecutor(true),
                null,
                true, /* deserialize record set */
                isHandleForReading);
    }

    synchronized BKLogReadHandler createReadHandler(Optional<String> subscriberId,
                                                    OrderedScheduler lockExecutor,
                                                    AsyncNotification notification,
                                                    boolean deserializeRecordSet,
                                                    boolean isHandleForReading) {
        ZKLogMetadataForReader logMetadata = ZKLogMetadataForReader.of(uri, name, streamIdentifier);
        return new BKLogReadHandler(
                logMetadata,
                subscriberId,
                conf,
                dynConf,
                readerZKCBuilder,
                readerBKCBuilder,
                readerMetadataStore,
                scheduler,
                lockExecutor,
                readAheadScheduler,
                alertStatsLogger,
                readAheadExceptionsLogger,
                statsLogger,
                perLogStatsLogger,
                clientId,
                notification,
                isHandleForReading,
                deserializeRecordSet);
    }

    // Create Ledger Allocator

    LedgerAllocator createLedgerAllocator(ZKLogMetadataForWriter logMetadata) throws IOException {
        LedgerAllocator ledgerAllocatorDelegator;
        if (!dynConf.getEnableLedgerAllocatorPool()) {
            QuorumConfigProvider quorumConfigProvider =
                    new DynamicQuorumConfigProvider(dynConf);
            LedgerAllocator allocator = new SimpleLedgerAllocator(
                    logMetadata.getAllocationPath(),
                    logMetadata.getAllocationData(),
                    quorumConfigProvider,
                    writerZKC,
                    writerBKC);
            ledgerAllocatorDelegator = new LedgerAllocatorDelegator(allocator, true);
        } else {
            ledgerAllocatorDelegator = ledgerAllocator;
        }
        return ledgerAllocatorDelegator;
    }

    // Create Write Handler

    public BKLogWriteHandler createWriteHandler(boolean lockHandler)
            throws IOException {
        return FutureUtils.result(asyncCreateWriteHandler(lockHandler));
    }

    Future<BKLogWriteHandler> asyncCreateWriteHandler(final boolean lockHandler) {
        final ZooKeeper zk;
        try {
            zk = writerZKC.get();
        } catch (InterruptedException e) {
            LOG.error("Failed to initialize zookeeper client : ", e);
            return Future.exception(new DLInterruptedException("Failed to initialize zookeeper client", e));
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return Future.exception(FutureUtils.zkException(e, uri.getPath()));
        }

        boolean ownAllocator = null == ledgerAllocator;

        // Fetching Log Metadata
        Future<ZKLogMetadataForWriter> metadataFuture =
                ZKLogMetadataForWriter.of(uri, name, streamIdentifier,
                        zk, writerZKC.getDefaultACL(),
                        ownAllocator, conf.getCreateStreamIfNotExists() || ownAllocator);
        return metadataFuture.flatMap(new AbstractFunction1<ZKLogMetadataForWriter, Future<BKLogWriteHandler>>() {
            @Override
            public Future<BKLogWriteHandler> apply(ZKLogMetadataForWriter logMetadata) {
                Promise<BKLogWriteHandler> createPromise = new Promise<BKLogWriteHandler>();
                createWriteHandler(logMetadata, lockHandler, createPromise);
                return createPromise;
            }
        });
    }

    private void createWriteHandler(ZKLogMetadataForWriter logMetadata,
                                    boolean lockHandler,
                                    final Promise<BKLogWriteHandler> createPromise) {
        OrderedScheduler lockStateExecutor = getLockStateExecutor(true);
        // Build the locks
        DistributedLock lock = new DistributedLock(
                lockStateExecutor,
                getLockFactory(true),
                logMetadata.getLockPath(),
                conf.getLockTimeoutMilliSeconds(),
                statsLogger);
        // Build the ledger allocator
        LedgerAllocator allocator;
        try {
            allocator = createLedgerAllocator(logMetadata);
        } catch (IOException e) {
            FutureUtils.setException(createPromise, e);
            return;
        }

        // Make sure writer handler created before resources are initialized
        final BKLogWriteHandler writeHandler = new BKLogWriteHandler(
                logMetadata,
                conf,
                writerZKCBuilder,
                writerBKCBuilder,
                writerMetadataStore,
                scheduler,
                allocator,
                statsLogger,
                perLogStatsLogger,
                alertStatsLogger,
                clientId,
                regionId,
                writeLimiter,
                featureProvider,
                dynConf,
                lock);
        PermitManager manager = getLogSegmentRollingPermitManager();
        if (manager instanceof Watcher) {
            writeHandler.register((Watcher) manager);
        }
        if (lockHandler) {
            writeHandler.lockHandler().addEventListener(new FutureEventListener<DistributedLock>() {
                @Override
                public void onSuccess(DistributedLock lock) {
                    FutureUtils.setValue(createPromise, writeHandler);
                }

                @Override
                public void onFailure(final Throwable cause) {
                    writeHandler.asyncClose().ensure(new AbstractFunction0<BoxedUnit>() {
                        @Override
                        public BoxedUnit apply() {
                            FutureUtils.setException(createPromise, cause);
                            return BoxedUnit.UNIT;
                        }
                    });
                }
            });
        } else {
            FutureUtils.setValue(createPromise, writeHandler);
        }
    }

    PermitManager getLogSegmentRollingPermitManager() {
        return logSegmentRollingPermitManager;
    }

    <T> Future<T> processReaderOperation(final Function<BKLogReadHandler, Future<T>> func) {
        initializeFuturePool(false);
        return readerFuturePool.apply(new ExceptionalFunction0<BKLogReadHandler>() {
            @Override
            public BKLogReadHandler applyE() throws Throwable {
                return getReadHandlerForListener(true);
            }
        }).flatMap(new ExceptionalFunction<BKLogReadHandler, Future<T>>() {
            @Override
            public Future<T> applyE(final BKLogReadHandler readHandler) throws Throwable {
                return func.apply(readHandler);
            }
        });
    }

    /**
     * Check if an end of stream marker was added to the stream
     * A stream with an end of stream marker cannot be appended to
     *
     * @return true if the marker was added to the stream, false otherwise
     */
    @Override
    public boolean isEndOfStreamMarked() throws IOException {
        checkClosedOrInError("isEndOfStreamMarked");
        long lastTxId = FutureUtils.result(getLastLogRecordAsyncInternal(false, true)).getTransactionId();
        return lastTxId == DistributedLogConstants.MAX_TXID;
    }

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException {
        long position;
        try {
            position = FutureUtils.result(getLastLogRecordAsyncInternal(true, false)).getTransactionId();
            if (DistributedLogConstants.INVALID_TXID == position ||
                DistributedLogConstants.EMPTY_LOGSEGMENT_TX_ID == position) {
                position = 0;
            }
        } catch (LogEmptyException ex) {
            position = 0;
        } catch (LogNotFoundException ex) {
            position = 0;
        }
        return new AppendOnlyStreamWriter(startAsyncLogSegmentNonPartitioned(), position);
    }

    /**
     * Get a reader to read a log stream as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamReader getAppendOnlyStreamReader() throws IOException {
        return new AppendOnlyStreamReader(this);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public BKSyncLogWriter startLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        BKSyncLogWriter writer = new BKSyncLogWriter(conf, dynConf, this);
        boolean success = false;
        try {
            writer.createAndCacheWriteHandler();
            BKLogWriteHandler writeHandler = writer.getWriteHandler();
            FutureUtils.result(writeHandler.lockHandler());
            success = true;
            return writer;
        } finally {
            if (!success) {
                writer.abort();
            }
        }
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public BKAsyncLogWriter startAsyncLogSegmentNonPartitioned() throws IOException {
        return (BKAsyncLogWriter) FutureUtils.result(openAsyncLogWriter());
    }

    @Override
    public Future<AsyncLogWriter> openAsyncLogWriter() {
        try {
            checkClosedOrInError("startLogSegmentNonPartitioned");
        } catch (AlreadyClosedException e) {
            return Future.exception(e);
        }

        Future<BKLogWriteHandler> createWriteHandleFuture;
        synchronized (this) {
            // 1. create the locked write handler
            createWriteHandleFuture = asyncCreateWriteHandler(true);
        }
        return createWriteHandleFuture.flatMap(new AbstractFunction1<BKLogWriteHandler, Future<AsyncLogWriter>>() {
            @Override
            public Future<AsyncLogWriter> apply(final BKLogWriteHandler writeHandler) {
                final BKAsyncLogWriter writer;
                synchronized (BKDistributedLogManager.this) {
                    // 2. create the writer with the handler
                    writer = new BKAsyncLogWriter(
                            conf,
                            dynConf,
                            BKDistributedLogManager.this,
                            writeHandler,
                            featureProvider,
                            statsLogger);
                }
                // 3. recover the incomplete log segments
                return writeHandler.recoverIncompleteLogSegments()
                        .map(new AbstractFunction1<Long, AsyncLogWriter>() {
                            @Override
                            public AsyncLogWriter apply(Long lastTxId) {
                                // 4. update last tx id if successfully recovered
                                writer.setLastTxId(lastTxId);
                                return writer;
                            }
                        }).onFailure(new AbstractFunction1<Throwable, BoxedUnit>() {
                            @Override
                            public BoxedUnit apply(Throwable cause) {
                                // 5. close the writer if recovery failed
                                writer.asyncAbort();
                                return BoxedUnit.UNIT;
                            }
                        });
            }
        });
    }

    @Override
    public Future<DLSN> getDLSNNotLessThanTxId(final long fromTxnId) {
        return getLogSegmentsAsync().flatMap(new AbstractFunction1<List<LogSegmentMetadata>, Future<DLSN>>() {
            @Override
            public Future<DLSN> apply(List<LogSegmentMetadata> segments) {
                return getDLSNNotLessThanTxId(fromTxnId, segments);
            }
        });
    }

    private Future<DLSN> getDLSNNotLessThanTxId(long fromTxnId,
                                                final List<LogSegmentMetadata> segments) {
        if (segments.isEmpty()) {
            return getLastDLSNAsync();
        }
        final int segmentIdx = DLUtils.findLogSegmentNotLessThanTxnId(segments, fromTxnId);
        if (segmentIdx < 0) {
            return Future.value(new DLSN(segments.get(0).getLogSegmentSequenceNumber(), 0L, 0L));
        }
        final LedgerHandleCache handleCache =
                LedgerHandleCache.newBuilder().bkc(readerBKC).conf(conf).build();
        return getDLSNNotLessThanTxIdInSegment(
                fromTxnId,
                segmentIdx,
                segments,
                handleCache
        ).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
            }
        });
    }

    private Future<DLSN> getDLSNNotLessThanTxIdInSegment(final long fromTxnId,
                                                         final int segmentIdx,
                                                         final List<LogSegmentMetadata> segments,
                                                         final LedgerHandleCache handleCache) {
        final LogSegmentMetadata segment = segments.get(segmentIdx);
        return ReadUtils.getLogRecordNotLessThanTxId(
                name,
                segment,
                fromTxnId,
                scheduler,
                handleCache,
                Math.max(2, dynConf.getReadAheadBatchSize())
        ).flatMap(new AbstractFunction1<Optional<LogRecordWithDLSN>, Future<DLSN>>() {
            @Override
            public Future<DLSN> apply(Optional<LogRecordWithDLSN> foundRecord) {
                if (foundRecord.isPresent()) {
                    return Future.value(foundRecord.get().getDlsn());
                }
                if ((segments.size() - 1) == segmentIdx) {
                    return getLastLogRecordAsync().map(new AbstractFunction1<LogRecordWithDLSN, DLSN>() {
                        @Override
                        public DLSN apply(LogRecordWithDLSN record) {
                            if (record.getTransactionId() >= fromTxnId) {
                                return record.getDlsn();
                            }
                            return record.getDlsn().getNextDLSN();
                        }
                    });
                } else {
                    return getDLSNNotLessThanTxIdInSegment(
                            fromTxnId,
                            segmentIdx + 1,
                            segments,
                            handleCache);
                }
            }
        });
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public LogReader getInputStream(long fromTxnId)
        throws IOException {
        return getInputStreamInternal(fromTxnId);
    }

    @Override
    public LogReader getInputStream(DLSN fromDLSN) throws IOException {
        return getInputStreamInternal(fromDLSN, Optional.<Long>absent());
    }

    @Override
    public AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException {
        return FutureUtils.result(openAsyncLogReader(fromTxnId));
    }

    /**
     * Opening a log reader positioning by transaction id <code>fromTxnId</code>.
     *
     * <p>
     * - retrieve log segments for the stream
     * - if the log segment list is empty, positioning by the last dlsn
     * - otherwise, find the first log segment that contains the records whose transaction ids are not less than
     *   the provided transaction id <code>fromTxnId</code>
     *   - if all log segments' records' transaction ids are more than <code>fromTxnId</code>, positioning
     *     on the first record.
     *   - otherwise, search the log segment to find the log record
     *     - if the log record is found, positioning the reader by that found record's dlsn
     *     - otherwise, positioning by the last dlsn
     * </p>
     *
     * @see DLUtils#findLogSegmentNotLessThanTxnId(List, long)
     * @see ReadUtils#getLogRecordNotLessThanTxId(String, LogSegmentMetadata, long, ExecutorService, LedgerHandleCache, int)
     * @param fromTxnId
     *          transaction id to start reading from
     * @return future representing the open result.
     */
    @Override
    public Future<AsyncLogReader> openAsyncLogReader(long fromTxnId) {
        final Promise<DLSN> dlsnPromise = new Promise<DLSN>();
        getDLSNNotLessThanTxId(fromTxnId).addEventListener(new FutureEventListener<DLSN>() {

            @Override
            public void onSuccess(DLSN dlsn) {
                dlsnPromise.setValue(dlsn);
            }

            @Override
            public void onFailure(Throwable cause) {
                if (cause instanceof LogEmptyException) {
                    dlsnPromise.setValue(DLSN.InitialDLSN);
                } else {
                    dlsnPromise.setException(cause);
                }
            }
        });
        return dlsnPromise.flatMap(new AbstractFunction1<DLSN, Future<AsyncLogReader>>() {
            @Override
            public Future<AsyncLogReader> apply(DLSN dlsn) {
                return openAsyncLogReader(dlsn);
            }
        });
    }

    @Override
    public AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException {
        return FutureUtils.result(openAsyncLogReader(fromDLSN));
    }

    @Override
    public Future<AsyncLogReader> openAsyncLogReader(DLSN fromDLSN) {
        Optional<String> subscriberId = Optional.absent();
        AsyncLogReader reader = new BKAsyncLogReaderDLSN(
                this,
                scheduler,
                getLockStateExecutor(true),
                fromDLSN,
                subscriberId,
                false,
                dynConf.getDeserializeRecordSetOnReads(),
                statsLogger);
        return Future.value(reader);
    }

    /**
     * Note the lock here is a sort of elective exclusive lock. I.e. acquiring this lock will only prevent other
     * people who try to acquire the lock from reading from the stream. Normal readers (and writers) will not be
     * blocked.
     */
    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(final DLSN fromDLSN) {
        Optional<String> subscriberId = Optional.absent();
        return getAsyncLogReaderWithLock(Optional.of(fromDLSN), subscriberId);
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(final DLSN fromDLSN, final String subscriberId) {
        return getAsyncLogReaderWithLock(Optional.of(fromDLSN), Optional.of(subscriberId));
    }

    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(String subscriberId) {
        Optional<DLSN> fromDLSN = Optional.absent();
        return getAsyncLogReaderWithLock(fromDLSN, Optional.of(subscriberId));
    }

    protected Future<AsyncLogReader> getAsyncLogReaderWithLock(final Optional<DLSN> fromDLSN,
                                                               final Optional<String> subscriberId) {
        if (!fromDLSN.isPresent() && !subscriberId.isPresent()) {
            return Future.exception(new UnexpectedException("Neither from dlsn nor subscriber id is provided."));
        }
        final BKAsyncLogReaderDLSN reader = new BKAsyncLogReaderDLSN(
                BKDistributedLogManager.this,
                scheduler,
                getLockStateExecutor(true),
                fromDLSN.isPresent() ? fromDLSN.get() : DLSN.InitialDLSN,
                subscriberId,
                false,
                dynConf.getDeserializeRecordSetOnReads(),
                statsLogger);
        pendingReaders.add(reader);
        final Future<Void> lockFuture = reader.lockStream();
        final Promise<AsyncLogReader> createPromise = new Promise<AsyncLogReader>(
                new Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable cause) {
                // cancel the lock when the creation future is cancelled
                lockFuture.cancel();
                return BoxedUnit.UNIT;
            }
        });
        // lock the stream - fetch the last commit position on success
        lockFuture.flatMap(new Function<Void, Future<AsyncLogReader>>() {
            @Override
            public Future<AsyncLogReader> apply(Void complete) {
                if (fromDLSN.isPresent()) {
                    return Future.value((AsyncLogReader) reader);
                }
                LOG.info("Reader {} @ {} reading last commit position from subscription store after acquired lock.",
                        subscriberId.get(), name);
                // we acquired lock
                final SubscriptionStateStore stateStore = getSubscriptionStateStore(subscriberId.get());
                return stateStore.getLastCommitPosition().map(new ExceptionalFunction<DLSN, AsyncLogReader>() {
                    @Override
                    public AsyncLogReader applyE(DLSN lastCommitPosition) throws UnexpectedException {
                        LOG.info("Reader {} @ {} positioned to last commit position {}.",
                                new Object[] { subscriberId.get(), name, lastCommitPosition });
                        reader.setStartDLSN(lastCommitPosition);
                        return reader;
                    }
                });
            }
        }).addEventListener(new FutureEventListener<AsyncLogReader>() {
            @Override
            public void onSuccess(AsyncLogReader r) {
                pendingReaders.remove(reader);
                FutureUtils.setValue(createPromise, r);
            }

            @Override
            public void onFailure(final Throwable cause) {
                pendingReaders.remove(reader);
                reader.asyncClose().ensure(new AbstractFunction0<BoxedUnit>() {
                    @Override
                    public BoxedUnit apply() {
                        FutureUtils.setException(createPromise, cause);
                        return BoxedUnit.UNIT;
                    }
                });
            }
        });
        return createPromise;
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param fromTxnId
     *          transaction id to start reading from
     * @return log reader
     * @throws IOException
     */
    LogReader getInputStreamInternal(long fromTxnId)
        throws IOException {
        DLSN fromDLSN;
        try {
            fromDLSN = FutureUtils.result(getDLSNNotLessThanTxId(fromTxnId));
        } catch (LogEmptyException lee) {
            fromDLSN = DLSN.InitialDLSN;
        }
        return getInputStreamInternal(fromDLSN, Optional.of(fromTxnId));
    }

    LogReader getInputStreamInternal(DLSN fromDLSN, Optional<Long> fromTxnId)
            throws IOException {
        LOG.info("Create async reader starting from {}", fromDLSN);
        checkClosedOrInError("getInputStream");
        Optional<String> subscriberId = Optional.absent();
        BKAsyncLogReaderDLSN asyncReader = new BKAsyncLogReaderDLSN(
                this,
                scheduler,
                getLockStateExecutor(true),
                fromDLSN,
                subscriberId,
                true,
                dynConf.getDeserializeRecordSetOnReads(),
                statsLogger);
        return new BKSyncLogReaderDLSN(conf, asyncReader, scheduler, fromTxnId);
    }

    /**
     * Get the last log record in the stream
     *
     * @return the last log record in the stream
     * @throws java.io.IOException if a stream cannot be found.
     */
    @Override
    public LogRecordWithDLSN getLastLogRecord() throws IOException {
        checkClosedOrInError("getLastLogRecord");
        return FutureUtils.result(getLastLogRecordAsync());
    }

    @Override
    public long getFirstTxId() throws IOException {
        checkClosedOrInError("getFirstTxId");
        return FutureUtils.result(getFirstRecordAsyncInternal()).getTransactionId();
    }

    @Override
    public long getLastTxId() throws IOException {
        checkClosedOrInError("getLastTxId");
        return FutureUtils.result(getLastTxIdAsync());
    }

    @Override
    public DLSN getLastDLSN() throws IOException {
        checkClosedOrInError("getLastDLSN");
        return FutureUtils.result(getLastLogRecordAsyncInternal(false, false)).getDlsn();
    }

    /**
     * Get Latest log record in the log
     *
     * @return latest log record
     */
    @Override
    public Future<LogRecordWithDLSN> getLastLogRecordAsync() {
        return getLastLogRecordAsyncInternal(false, false);
    }

    private Future<LogRecordWithDLSN> getLastLogRecordAsyncInternal(final boolean recover,
                                                                    final boolean includeEndOfStream) {
        return processReaderOperation(new Function<BKLogReadHandler, Future<LogRecordWithDLSN>>() {
            @Override
            public Future<LogRecordWithDLSN> apply(final BKLogReadHandler ledgerHandler) {
                return ledgerHandler.getLastLogRecordAsync(recover, includeEndOfStream);
            }
        });
    }

    /**
     * Get Latest Transaction Id in the log
     *
     * @return latest transaction id
     */
    @Override
    public Future<Long> getLastTxIdAsync() {
        return getLastLogRecordAsyncInternal(false, false)
                .map(RECORD_2_TXID_FUNCTION);
    }

    /**
     * Get first DLSN in the log.
     *
     * @return first dlsn in the stream
     */
    @Override
    public Future<DLSN> getFirstDLSNAsync() {
        return getFirstRecordAsyncInternal().map(RECORD_2_DLSN_FUNCTION);
    }

    private Future<LogRecordWithDLSN> getFirstRecordAsyncInternal() {
        return processReaderOperation(new Function<BKLogReadHandler, Future<LogRecordWithDLSN>>() {
            @Override
            public Future<LogRecordWithDLSN> apply(final BKLogReadHandler ledgerHandler) {
                return ledgerHandler.asyncGetFirstLogRecord();
            }
        });
    }

    /**
     * Get Latest DLSN in the log.
     *
     * @return latest transaction id
     */
    @Override
    public Future<DLSN> getLastDLSNAsync() {
        return getLastLogRecordAsyncInternal(false, false)
                .map(RECORD_2_DLSN_FUNCTION);
    }

    /**
     * Get the number of log records in the active portion of the log
     * Any log segments that have already been truncated will not be included
     *
     * @return number of log records
     * @throws IOException
     */
    @Override
    public long getLogRecordCount() throws IOException {
        checkClosedOrInError("getLogRecordCount");
        return FutureUtils.result(getLogRecordCountAsync(DLSN.InitialDLSN));
    }

    /**
     * Get the number of log records in the active portion of the log asynchronously.
     * Any log segments that have already been truncated will not be included
     *
     * @return future number of log records
     * @throws IOException
     */
    @Override
    public Future<Long> getLogRecordCountAsync(final DLSN beginDLSN) {
        return processReaderOperation(new Function<BKLogReadHandler, Future<Long>>() {
                    @Override
                    public Future<Long> apply(BKLogReadHandler ledgerHandler) {
                        return ledgerHandler.asyncGetLogRecordCount(beginDLSN);
                    }
                });
    }

    @Override
    public void recover() throws IOException {
        recoverInternal(conf.getUnpartitionedStreamName());
    }

    /**
     * Recover a specified stream within the log container
     * The writer implicitly recovers a topic when it resumes writing.
     * This allows applications to recover a container explicitly so
     * that application may read a fully recovered log before resuming
     * the writes
     *
     * @throws IOException if the recovery fails
     */
    private void recoverInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("recoverInternal");
        BKLogWriteHandler ledgerHandler = createWriteHandler(true);
        try {
            FutureUtils.result(ledgerHandler.recoverIncompleteLogSegments());
        } finally {
            Utils.closeQuietly(ledgerHandler);
        }
    }

    /**
     * Delete all the partitions of the specified log
     *
     * @throws IOException if the deletion fails
     */
    @Override
    public void delete() throws IOException {
        BKLogWriteHandler ledgerHandler = createWriteHandler(true);
        try {
            ledgerHandler.deleteLog();
        } finally {
            Utils.closeQuietly(ledgerHandler);
        }

        // Delete the ZK path associated with the log stream
        String zkPath = getZKPath();
        // Safety check when we are using the shared zookeeper
        if (zkPath.toLowerCase().contains("distributedlog")) {
            try {
                LOG.info("Delete the path associated with the log {}, ZK Path {}", name, zkPath);
                ZKUtil.deleteRecursive(writerZKC.get(), zkPath);
            } catch (InterruptedException ie) {
                LOG.error("Interrupted while accessing ZK", ie);
                throw new DLInterruptedException("Error initializing zk", ie);
            } catch (KeeperException ke) {
                LOG.error("Error accessing entry in zookeeper", ke);
                throw new IOException("Error initializing zk", ke);
            }
        } else {
            LOG.warn("Skip deletion of unrecognized ZK Path {}", zkPath);
        }
    }


    /**
     * The DistributedLogManager may archive/purge any logs for transactionId
     * less than or equal to minImageTxId.
     * This is to be used only when the client explicitly manages deletion. If
     * the cleanup policy is based on sliding time window, then this method need
     * not be called.
     *
     * @param minTxIdToKeep the earliest txid that must be retained
     * @throws IOException if purging fails
     */
    @Override
    public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
        Preconditions.checkArgument(minTxIdToKeep > 0, "Invalid transaction id " + minTxIdToKeep);
        checkClosedOrInError("purgeLogSegmentsOlderThan");
        BKLogWriteHandler ledgerHandler = createWriteHandler(true);
        try {
            LOG.info("Purging logs for {} older than {}", ledgerHandler.getFullyQualifiedName(), minTxIdToKeep);
            FutureUtils.result(ledgerHandler.purgeLogSegmentsOlderThanTxnId(minTxIdToKeep));
        } finally {
            Utils.closeQuietly(ledgerHandler);
        }
    }

    static class PendingReaders implements AsyncCloseable {

        final ExecutorService executorService;
        final Set<AsyncLogReader> readers = new HashSet<AsyncLogReader>();

        PendingReaders(ExecutorService executorService) {
            this.executorService = executorService;
        }

        public synchronized void remove(AsyncLogReader reader) {
            readers.remove(reader);
        }

        public synchronized void add(AsyncLogReader reader) {
            readers.add(reader);
        }

        @Override
        public Future<Void> asyncClose() {
            return Utils.closeSequence(executorService, true, readers.toArray(new AsyncLogReader[readers.size()]))
                    .onSuccess(new AbstractFunction1<Void, BoxedUnit>() {
                        @Override
                        public BoxedUnit apply(Void value) {
                            readers.clear();
                            return BoxedUnit.UNIT;
                        }
                    });
        }
    };

    /**
     * Close the distributed log manager, freeing any resources it may hold.
     */
    @Override
    public Future<Void> asyncClose() {
        Promise<Void> closeFuture;
        BKLogReadHandler readHandlerToClose;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new Promise<Void>();
            readHandlerToClose = readHandlerForListener;
        }

        // NOTE: the resources {scheduler, writerBKC, readerBKC} are mostly from namespace instance.
        //       so they are not blocking call except tests.
        AsyncCloseable resourcesCloseable = new AsyncCloseable() {
            @Override
            public Future<Void> asyncClose() {
                int schedTimeout = conf.getSchedulerShutdownTimeoutMs();

                // Clean up executor state.
                if (ownExecutor) {
                    SchedulerUtils.shutdownScheduler(scheduler, schedTimeout, TimeUnit.MILLISECONDS);
                    LOG.info("Stopped BKDL executor service for {}.", name);

                    if (scheduler != readAheadScheduler) {
                        SchedulerUtils.shutdownScheduler(readAheadScheduler, schedTimeout, TimeUnit.MILLISECONDS);
                        LOG.info("Stopped BKDL ReadAhead Executor Service for {}.", name);
                    }

                    SchedulerUtils.shutdownScheduler(getLockStateExecutor(false), schedTimeout, TimeUnit.MILLISECONDS);
                    LOG.info("Stopped BKDL Lock State Executor for {}.", name);
                }
                if (ownWriterBKC) {
                    writerBKC.close();
                }
                if (ownReaderBKC) {
                    readerBKC.close();
                }
                return Future.Void();
            }
        };

        Future<Void> closeResult = Utils.closeSequence(null, true,
                readHandlerToClose,
                pendingReaders,
                resourcesCloseable,
                new AsyncCloseable() {
                    @Override
                    public Future<Void> asyncClose() {
                        return BKDistributedLogManager.super.asyncClose();
                    }
                });
        closeResult.proxyTo(closeFuture);
        return closeFuture;
    }

    @Override
    public void close() throws IOException {
        FutureUtils.result(asyncClose());
    }

    public boolean scheduleTask(Runnable task) {
        try {
            scheduler.submit(task);
            return true;
        } catch (RejectedExecutionException ree) {
            LOG.error("Task {} is rejected : ", task, ree);
            return false;
        }
    }

    private FuturePool buildFuturePool(ExecutorService executorService,
                                       StatsLogger statsLogger) {
        FuturePool futurePool = new ExecutorServiceFuturePool(executorService);
        return new MonitoredFuturePool(
                futurePool,
                statsLogger,
                conf.getEnableTaskExecutionStats(),
                conf.getTaskExecutionWarnTimeMicros());
    }

    private void initializeFuturePool(boolean ordered) {
        // ownExecutor is a single threaded thread pool
        if (null == readerFuturePool) {
            readerFuturePool = buildFuturePool(
                    scheduler, statsLogger.scope("reader_future_pool"));
        }
    }

    @Override
    public String toString() {
        return String.format("DLM:%s:%s", getZKPath(), getStreamName());
    }

    public void raiseAlert(String msg, Object... args) {
        alertStatsLogger.raise(msg, args);
    }

    /**
     * Get the subscription state storage provided by the distributed log manager
     *
     * @param subscriberId - Application specific Id associated with the subscriber
     * @return Subscription state store
     */
    @Override
    @Deprecated
    public SubscriptionStateStore getSubscriptionStateStore(String subscriberId) {
        return getSubscriptionStateStoreInternal(conf.getUnpartitionedStreamName(), subscriberId);
    }

    /**
     * Get the subscription state storage provided by the distributed log manager
     *
     * @param streamIdentifier - Identifier associated with the stream
     * @param subscriberId - Application specific Id associated with the subscriber
     * @return Subscription state store
     */
    private SubscriptionStateStore getSubscriptionStateStoreInternal(String streamIdentifier, String subscriberId) {
        return new ZKSubscriptionStateStore(writerZKC,
                ZKLogMetadataForReader.getSubscriberPath(uri, name, streamIdentifier, subscriberId));
    }

    @Override
    public SubscriptionsStore getSubscriptionsStore() {
        return getSubscriptionsStoreInternal(conf.getUnpartitionedStreamName());
    }

    /**
     * Get the subscription state storage provided by the distributed log manager
     *
     * @param streamIdentifier - Identifier associated with the stream
     * @return Subscriptions store
     */
    private SubscriptionsStore getSubscriptionsStoreInternal(String streamIdentifier) {
        return new ZKSubscriptionsStore(writerZKC,
                ZKLogMetadataForReader.getSubscribersPath(uri, name, streamIdentifier));
    }
}
