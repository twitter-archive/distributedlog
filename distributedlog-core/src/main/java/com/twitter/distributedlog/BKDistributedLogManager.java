package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.stats.ReadAheadExceptionsLogger;
import com.twitter.distributedlog.subscription.SubscriptionStateStore;
import com.twitter.distributedlog.subscription.SubscriptionsStore;
import com.twitter.distributedlog.subscription.ZKSubscriptionStateStore;
import com.twitter.distributedlog.subscription.ZKSubscriptionsStore;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.distributedlog.util.MonitoredFuturePool;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.distributedlog.zk.DataWithStat;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.FutureEventListener;

import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class BKDistributedLogManager extends ZKMetadataAccessor implements DistributedLogManager {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogManager.class);

    static String getPartitionPath(URI uri, String streamName, String streamIdentifier) {
        return String.format("%s/%s/%s", uri.getPath(), streamName, streamIdentifier);
    }

    static void createUnpartitionedStream(DistributedLogConfiguration conf, ZooKeeperClient zkc, URI uri, String streamName) throws IOException, InterruptedException {
        BKLogPartitionWriteHandler.createStreamIfNotExists(streamName, getPartitionPath(uri, streamName,
            conf.getUnpartitionedStreamName()), zkc.get(), zkc.getDefaultACL(), true, new DataWithStat(), new DataWithStat(), new DataWithStat());
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
    private final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    private boolean closed = true;
    private final OrderedScheduler scheduler;
    private final ScheduledExecutorService readAheadExecutor;
    private boolean ownExecutor;
    private final FeatureProvider featureProvider;
    private final StatsLogger statsLogger;
    private final AlertStatsLogger alertStatsLogger;

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
    private ExecutorService writerFuturePoolExecutorService = null;
    private FuturePool writerFuturePool = null;
    private OrderedSafeExecutor lockStateExecutor = null;
    // writer stats
    private final OpStatsLogger createWriteHandlerStats;

    //
    // Reader Related Variables
    ///
    // read handler for listener.
    private BKLogPartitionReadHandler readHandlerForListener = null;
    private FuturePool readerFuturePool = null;
    private final PendingReaders pendingReaders;
    private final ReadAheadExceptionsLogger readAheadExceptionsLogger;

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
             statsLogger);
        this.ownExecutor = true;
    }

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
                            OrderedScheduler scheduler,
                            ScheduledExecutorService readAheadExecutor,
                            OrderedSafeExecutor lockStateExecutor,
                            ClientSocketChannelFactory channelFactory,
                            HashedWheelTimer requestTimer,
                            ReadAheadExceptionsLogger readAheadExceptionsLogger,
                            String clientId,
                            Integer regionId,
                            LedgerAllocator ledgerAllocator,
                            PermitLimiter writeLimiter,
                            PermitManager logSegmentRollingPermitManager,
                            FeatureProvider featureProvider,
                            StatsLogger statsLogger) throws IOException {
        super(name, conf, uri, writerZKCBuilder, readerZKCBuilder, statsLogger);
        Preconditions.checkNotNull(readAheadExceptionsLogger, "No ReadAhead Stats Logger Provided.");
        this.conf = conf;
        this.dynConf = dynConf;
        this.scheduler = scheduler;
        this.lockStateExecutor = lockStateExecutor;
        this.readAheadExecutor = null == readAheadExecutor ? scheduler : readAheadExecutor;
        this.statsLogger = statsLogger;
        this.ownExecutor = false;
        this.pendingReaders = new PendingReaders();
        this.regionId = regionId;
        this.clientId = clientId;
        this.ledgerAllocator = ledgerAllocator;
        this.writeLimiter = writeLimiter;
        this.logSegmentRollingPermitManager = logSegmentRollingPermitManager;

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

        closed = false;

        // Feature Provider
        this.featureProvider = featureProvider;

        // Stats
        StatsLogger handlerStatsLogger = statsLogger.scope("handlers");
        this.createWriteHandlerStats = handlerStatsLogger.getOpStatsLogger("create_write_handler");
        this.alertStatsLogger = new AlertStatsLogger(statsLogger, name, "dl_alert");
        this.readAheadExceptionsLogger = readAheadExceptionsLogger;
    }

    private synchronized OrderedSafeExecutor getLockStateExecutor(boolean createIfNull) {
        if (createIfNull && null == lockStateExecutor && ownExecutor) {
            lockStateExecutor = OrderedSafeExecutor.newBuilder()
                    .numThreads(1).name("BKDL-LockState").build();
        }
        return lockStateExecutor;
    }

    DistributedLogConfiguration getConf() {
        return conf;
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

    @Override
    public synchronized List<LogSegmentMetadata> getLogSegments() throws IOException {
        BKLogPartitionReadHandler readHandler = createReadLedgerHandler(conf.getUnpartitionedStreamName());
        try {
            return readHandler.getFullLedgerList(true, false);
        } finally {
            readHandler.close();
        }
    }

    @Override
    public synchronized void registerListener(LogSegmentListener listener) throws IOException {
        if (null == readHandlerForListener) {
            readHandlerForListener = createReadLedgerHandler(conf.getUnpartitionedStreamName());
            readHandlerForListener.registerListener(listener);
            readHandlerForListener.scheduleGetLedgersTask(true, true);
        } else {
            readHandlerForListener.registerListener(listener);
        }
    }

    @Override
    public synchronized void unregisterListener(LogSegmentListener listener) {
        if (null != readHandlerForListener) {
            readHandlerForListener.unregisterListener(listener);
        }
    }

    public void checkClosedOrInError(String operation) throws AlreadyClosedException {
        if (closed) {
            throw new AlreadyClosedException("Executing " + operation + " on already closed DistributedLogManager");
        }

        if (null != writerBKC) {
            writerBKC.checkClosedOrInError();
        }
        if (null != readerBKC) {
            readerBKC.checkClosedOrInError();
        }
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier) throws IOException {
        Optional<String> subscriberId = Optional.absent();
        return createReadLedgerHandler(streamIdentifier, subscriberId, false);
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier, Optional<String> subscriberId)
            throws IOException {
        return createReadLedgerHandler(streamIdentifier, subscriberId, false);
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier,
                                                                          Optional<String> subscriberId, boolean isHandleForReading)
            throws IOException {
        return createReadLedgerHandler(streamIdentifier, subscriberId, getLockStateExecutor(true), null, isHandleForReading);
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier,
                                                                          Optional<String> subscriberId,
                                                                          OrderedSafeExecutor lockExecutor,
                                                                          AsyncNotification notification,
                                                                          boolean isHandleForReading) {
        return new BKLogPartitionReadHandler(name, streamIdentifier, subscriberId, conf, uri,
                readerZKCBuilder, readerBKCBuilder, scheduler, lockExecutor, readAheadExecutor,
                alertStatsLogger, readAheadExceptionsLogger, statsLogger, clientId, notification, isHandleForReading);
    }

    public BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier) throws IOException {
        return createWriteLedgerHandler(streamIdentifier, null);
    }

    BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier,
                                                        FuturePool orderedFuturePool)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            BKLogPartitionWriteHandler handler = doCreateWriteLedgerHandler(streamIdentifier, orderedFuturePool);
            success = true;
            return handler;
        } finally {
            if (success) {
                createWriteHandlerStats.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                createWriteHandlerStats.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    synchronized BKLogPartitionWriteHandler doCreateWriteLedgerHandler(String streamIdentifier,
                                                                       FuturePool orderedFuturePool)
            throws IOException {
        BKLogPartitionWriteHandler writeHandler =
            BKLogPartitionWriteHandler.createBKLogPartitionWriteHandler(name, streamIdentifier, conf, uri,
                writerZKCBuilder, writerBKCBuilder, scheduler, orderedFuturePool,
                getLockStateExecutor(true), ledgerAllocator, statsLogger, alertStatsLogger, clientId, regionId,
                writeLimiter, featureProvider, dynConf);
        PermitManager manager = getLogSegmentRollingPermitManager();
        if (manager instanceof Watcher) {
            writeHandler.register((Watcher) manager);
        }
        return writeHandler;
    }

    PermitManager getLogSegmentRollingPermitManager() {
        return logSegmentRollingPermitManager;
    }

    <T> Future<T> processReaderOperation(final String streamIdentifier,
                                         final Function<BKLogPartitionReadHandler, Future<T>> func) {
        initializeFuturePool(false);
        return readerFuturePool.apply(new ExceptionalFunction0<BKLogPartitionReadHandler>() {
            @Override
            public BKLogPartitionReadHandler applyE() throws Throwable {
                return createReadLedgerHandler(streamIdentifier);
            }
        }).flatMap(new ExceptionalFunction<BKLogPartitionReadHandler, Future<T>>() {
            @Override
            public Future<T> applyE(final BKLogPartitionReadHandler readHandler) throws Throwable {
                return func.apply(readHandler).ensure(new ExceptionalFunction0<BoxedUnit>() {
                    @Override
                    public BoxedUnit applyE() throws Throwable {
                        readHandler.close();
                        return BoxedUnit.UNIT;
                    }
                });
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
        return (getLastTxIdInternal(conf.getUnpartitionedStreamName(), false, true) == DistributedLogConstants.MAX_TXID);
    }

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes
     *
     * @return the writer interface to generate log records
     */
    public AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException {
        long position;
        try {
            position = getLastTxIdInternal(conf.getUnpartitionedStreamName(), true, false);
            if (DistributedLogConstants.INVALID_TXID == position ||
                DistributedLogConstants.EMPTY_LEDGER_TX_ID == position) {
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
    public synchronized BKUnPartitionedSyncLogWriter startLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        return new BKUnPartitionedSyncLogWriter(conf, dynConf, this);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public BKUnPartitionedAsyncLogWriter startAsyncLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        BKUnPartitionedAsyncLogWriter writer;
        synchronized (this) {
            initializeFuturePool(true);

            // proactively recover incomplete logsegments for async log writer
            writer = new BKUnPartitionedAsyncLogWriter(
                    conf, dynConf, this, writerFuturePool, featureProvider, statsLogger);
        }
        return writer.recover();
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
        return getInputStreamInternal(conf.getUnpartitionedStreamName(), fromTxnId);
    }

    @Override
    public LogReader getInputStream(DLSN fromDLSN) throws IOException {
        return getInputStreamInternal(conf.getUnpartitionedStreamName(), fromDLSN);
    }

    @Override
    public AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException {
        throw new NotYetImplementedException("getAsyncLogReader");
    }

    @Override
    public AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException {
        Optional<String> subscriberId = Optional.absent();
        return new BKAsyncLogReaderDLSN(this, scheduler, getLockStateExecutor(true),
                                        conf.getUnpartitionedStreamName(), fromDLSN, subscriberId,
                                        statsLogger);
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
            BKDistributedLogManager.this, scheduler,
            getLockStateExecutor(true), conf.getUnpartitionedStreamName(),
            fromDLSN.isPresent() ? fromDLSN.get() : DLSN.InitialDLSN, subscriberId, statsLogger);
        pendingReaders.add(reader);
        return reader.lockStream().flatMap(new Function<Void, Future<AsyncLogReader>>() {
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
            }

            @Override
            public void onFailure(Throwable cause) {
                pendingReaders.remove(reader);
                reader.close();
            }
        });
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param streamIdentifier
     * @param fromTxnId
     * @return
     * @throws IOException
     */
    public LogReader getInputStreamInternal(String streamIdentifier, long fromTxnId)
        throws IOException {
        checkClosedOrInError("getInputStream");
        return new BKContinuousLogReaderTxId(this, streamIdentifier, fromTxnId, conf, statsLogger);
    }

    LogReader getInputStreamInternal(String streamIdentifier, DLSN dlsn) throws IOException {
        checkClosedOrInError("getInputStream");
        return new BKContinuousLogReaderDLSN(this, streamIdentifier, dlsn, conf, statsLogger);
    }

    @Override
    public long getTxIdNotLaterThan(long thresholdTxId) throws IOException {
        return getTxIdNotLaterThanInternal(conf.getUnpartitionedStreamName(), thresholdTxId);
    }

    private long getTxIdNotLaterThanInternal(String streamIdentifier, long thresholdTxId) throws IOException {
        checkClosedOrInError("getTxIdNotLaterThan");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        long returnValue = ledgerHandler.getTxIdNotLaterThan(thresholdTxId);
        ledgerHandler.close();
        return returnValue;
    }

    /**
     * Get the last log record in the stream
     *
     * @return the last log record in the stream
     * @throws java.io.IOException if a stream cannot be found.
     */
    @Override
    public LogRecordWithDLSN getLastLogRecord() throws IOException {
        return getLastLogRecordInternal(conf.getUnpartitionedStreamName());
    }

    private LogRecordWithDLSN getLastLogRecordInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getLastLogRecord");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLastLogRecord(false, false);
        } finally {
            ledgerHandler.close();
        }
    }

    @Override
    public long getFirstTxId() throws IOException {
        return getFirstTxIdInternal(conf.getUnpartitionedStreamName());
    }

    private long getFirstTxIdInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getFirstTxId");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getFirstTxId();
        } finally {
            ledgerHandler.close();
        }
    }

    @Override
    public long getLastTxId() throws IOException {
        return getLastTxIdInternal(conf.getUnpartitionedStreamName(), false, false);
    }

    private long getLastTxIdInternal(String streamIdentifier, boolean recover, boolean includeEndOfStream) throws IOException {
        checkClosedOrInError("getLastTxId");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLastTxId(recover, includeEndOfStream);
        } finally {
            ledgerHandler.close();
        }
    }

    @Override
    public DLSN getLastDLSN() throws IOException {
        return getLastDLSNInternal(conf.getUnpartitionedStreamName(), false, false);
    }

    private DLSN getLastDLSNInternal(String streamIdentifier, boolean recover, boolean includeEndOfStream) throws IOException {
        checkClosedOrInError("getLastDLSN");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLastDLSN(recover, includeEndOfStream);
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Get Latest log record with DLSN in the non partitioned stream
     *
     * @return latest log record with DLSN
     */
    @Override
    public Future<LogRecordWithDLSN> getLastLogRecordAsync() {
        return getLastLogRecordAsyncInternal(conf.getUnpartitionedStreamName());
    }

    private Future<LogRecordWithDLSN> getLastLogRecordAsyncInternal(final String streamIdentifier) {
        return getLastRecordAsyncInternal(streamIdentifier, false, false);
    }

    private Future<LogRecordWithDLSN> getLastRecordAsyncInternal(final String streamIdentifier,
                                                                 final boolean recover,
                                                                 final boolean includeEndOfStream) {
        return processReaderOperation(streamIdentifier,
                new Function<BKLogPartitionReadHandler, Future<LogRecordWithDLSN>>() {
                    @Override
                    public Future<LogRecordWithDLSN> apply(final BKLogPartitionReadHandler ledgerHandler) {
                        return ledgerHandler.getLastLogRecordAsync(recover, includeEndOfStream);
                    }
                });
    }

    /**
     * Get Latest Transaction Id in the non partitioned stream
     *
     * @return latest transaction id
     */
    @Override
    public Future<Long> getLastTxIdAsync() {
        return getLastTxIdAsyncInternal(conf.getUnpartitionedStreamName());
    }

    private Future<Long> getLastTxIdAsyncInternal(final String streamIdentifier) {
        return getLastRecordAsyncInternal(streamIdentifier, false, false)
                .map(RECORD_2_TXID_FUNCTION);
    }

    /**
     * Get first DLSN in the unpartitioned stream.
     *
     * @return first dlsn in the stream
     */
    @Override
    public Future<DLSN> getFirstDLSNAsync() {
        return getFirstRecordAsyncInternal().map(RECORD_2_DLSN_FUNCTION);
    }

    public Future<LogRecordWithDLSN> getFirstRecordAsyncInternal() {
        return processReaderOperation(conf.getUnpartitionedStreamName(),
                new Function<BKLogPartitionReadHandler, Future<LogRecordWithDLSN>>() {
                    @Override
                    public Future<LogRecordWithDLSN> apply(final BKLogPartitionReadHandler ledgerHandler) {
                        return ledgerHandler.asyncGetFirstLogRecord();
                    }
                });
    }

    /**
     * Get Latest DLSN in the non partitioned stream
     *
     * @return latest transaction id
     */
    @Override
    public Future<DLSN> getLastDLSNAsync() {
        return getLastDLSNAsyncInternal(conf.getUnpartitionedStreamName());
    }

    private Future<DLSN> getLastDLSNAsyncInternal(final String streamIdentifier) {
        return getLastRecordAsyncInternal(streamIdentifier, false, false)
                .map(RECORD_2_DLSN_FUNCTION);
    }

    /**
     * Get the number of log records in the active portion of the non-partitioned
     * stream
     * Any log segments that have already been truncated will not be included
     *
     * @return number of log records
     * @throws IOException
     */
    @Override
    public long getLogRecordCount() throws IOException {
        return getLogRecordCountInternal(conf.getUnpartitionedStreamName());
    }

    private long getLogRecordCountInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("getLogRecordCount");
        BKLogPartitionReadHandler ledgerHandler = createReadLedgerHandler(streamIdentifier);
        try {
            return ledgerHandler.getLogRecordCount();
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Get the number of log records in the active portion of the non-partitioned
     * stream asynchronously.
     * Any log segments that have already been truncated will not be included
     *
     * @return future number of log records
     * @throws IOException
     */
    @Override
    public Future<Long> getLogRecordCountAsync(final DLSN beginDLSN) {
        return processReaderOperation(conf.getUnpartitionedStreamName(),
                new Function<BKLogPartitionReadHandler, Future<Long>>() {
            @Override
            public Future<Long> apply(BKLogPartitionReadHandler ledgerHandler) {
                return ledgerHandler.asyncGetLogRecordCount(beginDLSN);
            }
        });
    }

    /**
     * Recover the default stream within the log container (for
     * un partitioned log containers)
     *
     * @throws IOException if the recovery fails
     */
    @Override
    public void recover() throws IOException {
        recoverInternal(conf.getUnpartitionedStreamName());
    }

    /**
     * Recover a specified stream within the log container
     * The writer implicitly recovers a topic when it resumes writing.
     * This allows applications to recover a container explicitly so
     * that application may read a fully recovered partition before resuming
     * the writes
     *
     * @throws IOException if the recovery fails
     */
    private void recoverInternal(String streamIdentifier) throws IOException {
        checkClosedOrInError("recoverInternal");
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        try {
            ledgerHandler.recoverIncompleteLogSegments();
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Delete the specified partition
     *
     * @throws IOException if the deletion fails
     */
    public void deletePartition(String streamIdentifier) throws IOException {
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        try {
            ledgerHandler.deleteLog();
        } finally {
            ledgerHandler.close();
        }
    }

    /**
     * Delete all the partitions of the specified log
     *
     * @throws IOException if the deletion fails
     */
    @Override
    public void delete() throws IOException {
        for (String streamIdentifier : getStreamsWithinALog()) {
            deletePartition(streamIdentifier);
        }

        // Delete the ZK path associated with the log stream
        String zkPath = getZKPath();
        // Safety check when we are using the shared zookeeper
        if (zkPath.toLowerCase().contains("distributedlog")) {
            try {
                LOG.info("Delete the path associated with the log {}, ZK Path", name, zkPath);
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
        for (String streamIdentifier : getStreamsWithinALog()) {
            purgeLogsForPartitionOlderThan(streamIdentifier, minTxIdToKeep);
        }
    }

    private List<String> getStreamsWithinALog() throws IOException {
        List<String> partitions;
        String zkPath = getZKPath();
        try {
            if (readerZKC.get().exists(zkPath, false) == null) {
                LOG.info("Log {} was not found, ZK Path {} doesn't exist", name, zkPath);
                throw new LogNotFoundException("Log " + name + " was not found");
            }
            partitions = readerZKC.get().getChildren(zkPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
        }
        return partitions;
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
    public void purgeLogsForPartitionOlderThan(String streamIdentifier, long minTxIdToKeep) throws IOException {
        checkClosedOrInError("purgeLogsOlderThan");
        BKLogPartitionWriteHandler ledgerHandler = createWriteLedgerHandler(streamIdentifier);
        try {
            LOG.info("Purging logs for {} older than {}", ledgerHandler.getFullyQualifiedName(), minTxIdToKeep);
            ledgerHandler.purgeLogsOlderThan(minTxIdToKeep);
        } finally {
            ledgerHandler.close();
        }
    }

    static class PendingReaders {
        Set<AsyncLogReader> readers = new HashSet<AsyncLogReader>();
        public synchronized void remove(AsyncLogReader reader) {
            readers.remove(reader);
        }
        public synchronized void add(AsyncLogReader reader) {
            readers.add(reader);
        }
        public synchronized void close() {
            for (AsyncLogReader reader : readers) {
                try {
                    reader.close();
                } catch (Exception ex) {
                    LOG.error("failed to close pending reader");
                }
            }
            readers.clear();
        }
    };

    /**
     * Close the distributed log manager, freeing any resources it may hold.
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (null != readHandlerForListener) {
                readHandlerForListener.close();
            }
        }

        // Remove and close all pending readers.
        pendingReaders.close();

        int schedTimeout = conf.getSchedulerShutdownTimeoutMs();

        // Clean up executor state.
        if (ownExecutor) {
            SchedulerUtils.shutdownScheduler(scheduler, schedTimeout, TimeUnit.MILLISECONDS);
            LOG.info("Stopped BKDL executor service for {}.", name);

            if (scheduler != readAheadExecutor) {
                SchedulerUtils.shutdownScheduler(readAheadExecutor, schedTimeout, TimeUnit.MILLISECONDS);
                LOG.info("Stopped BKDL ReadAhead Executor Service for {}.", name);
            }

            SchedulerUtils.shutdownScheduler(getLockStateExecutor(false), schedTimeout, TimeUnit.MILLISECONDS);
            LOG.info("Stopped BKDL Lock State Executor for {}.", name);
        } else {
            if (null != writerFuturePoolExecutorService) {
                SchedulerUtils.shutdownScheduler(writerFuturePoolExecutorService, schedTimeout, TimeUnit.MILLISECONDS);
                LOG.info("Stopped Ordered Future Pool for {}.", name);
            }
        }
        if (ownWriterBKC) {
            writerBKC.close();
        }
        if (ownReaderBKC) {
            readerBKC.close();
        }
        try {
            super.close();
        } catch (IOException e) {
            LOG.warn("Exception while closing distributed log manager {} : ", name, e);
        }
        closed = true;
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

    private FuturePool buildFuturePool(ExecutorService executorService) {
        FuturePool futurePool = new ExecutorServiceFuturePool(executorService);
        MonitoredFuturePool monitoredFuturePool = new MonitoredFuturePool(
            futurePool, statsLogger.scope("ordered_future_pool"), conf.getEnableTaskExecutionStats(),
            conf.getTaskExecutionWarnTimeMicros());
        return monitoredFuturePool;
    }

    private void initializeFuturePool(boolean ordered) {
        // Note for writerFuturePool:
        // Single Threaded Future Pool inherently preserves order by tasks one by one
        //
        if (ownExecutor) {
            // ownExecutor is a single threaded thread pool
            if (null == writerFuturePool) {
                // Readers share the same future pool as the writerFuturePool
                writerFuturePool = buildFuturePool(scheduler);
                readerFuturePool = writerFuturePool;
            }
        } else if (ordered && (null == writerFuturePool)) {
            // When we are using a thread pool that was passed from the factory, we can use
            // the executor service
            writerFuturePoolExecutorService = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("BKALW-" + name + "-executor-%d")
                        .setDaemon(conf.getUseDaemonThread())
                        .build());
            writerFuturePool = buildFuturePool(writerFuturePoolExecutorService);
        } else if (!ordered && (null == readerFuturePool)) {
            // readerFuturePool can just use the executor service that was configured with the DLM
            readerFuturePool = buildFuturePool(scheduler);
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
            String.format("%s%s/%s", getPartitionPath(uri, name, streamIdentifier),
                    BKLogPartitionHandler.SUBSCRIBERS_PATH, subscriberId));
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
                String.format("%s%s", getPartitionPath(uri, name, streamIdentifier),
                        BKLogPartitionHandler.SUBSCRIBERS_PATH));
    }
}
