package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.NotYetImplementedException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.stats.AlertStatsLogger;
import com.twitter.distributedlog.stats.ReadAheadExceptionsLogger;
import com.twitter.distributedlog.subscription.SubscriptionStateStore;
import com.twitter.distributedlog.subscription.ZKSubscriptionStateStore;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.distributedlog.zk.DataWithStat;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.FutureEventListener;

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

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class BKDistributedLogManager extends ZKMetadataAccessor implements DistributedLogManager {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogManager.class);

    static String getPartitionPath(URI uri, String streamName, String streamIdentifier) {
        return String.format("%s/%s/%s", uri.getPath(), streamName, streamIdentifier);
    }

    static void createUnpartitionedStream(DistributedLogConfiguration conf, ZooKeeperClient zkc, URI uri, String streamName) throws IOException, InterruptedException {
        BKLogPartitionWriteHandler.createStreamIfNotExists(getPartitionPath(uri, streamName,
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

    private String clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
    private int regionId = DistributedLogConstants.LOCAL_REGION_ID;
    private final DistributedLogConfiguration conf;
    private boolean closed = true;
    private final ScheduledExecutorService executorService;
    private final ScheduledExecutorService readAheadExecutor;
    private boolean ownExecutor;

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
    private final StatsLogger statsLogger;
    private LedgerAllocator ledgerAllocator = null;
    // Log Segment Rolling Manager to control rolling speed
    private PermitManager logSegmentRollingPermitManager = PermitManager.UNLIMITED_PERMIT_MANAGER;
    // read handler for listener.
    private BKLogPartitionReadHandler readHandlerForListener = null;
    private ExecutorServiceFuturePool orderedFuturePool = null;
    private ExecutorServiceFuturePool readerFuturePool = null;
    private ExecutorService metadataExecutor = null;
    private OrderedSafeExecutor lockStateExecutor;

    private final ReadAheadExceptionsLogger readAheadExceptionsLogger;
    private final AlertStatsLogger alertStatsLogger;
    private final OpStatsLogger createWriteHandlerStats;

    final private PendingReaders pendingReaders;

    BKDistributedLogManager(String name,
                            DistributedLogConfiguration conf,
                            URI uri,
                            ZooKeeperClientBuilder writerZKCBuilder,
                            ZooKeeperClientBuilder readerZKCBuilder,
                            ZooKeeperClient zkcForWriterBKC,
                            ZooKeeperClient zkcForReaderBKC,
                            BookKeeperClientBuilder writerBKCBuilder,
                            BookKeeperClientBuilder readerBKCBuilder,
                            StatsLogger statsLogger) throws IOException {
        this(name, conf, uri,
             writerZKCBuilder, readerZKCBuilder,
             zkcForWriterBKC, zkcForReaderBKC, writerBKCBuilder, readerBKCBuilder,
             Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("BKDL-" + name + "-executor-%d").build()),
             null, null, null, null, new ReadAheadExceptionsLogger(statsLogger), statsLogger);
        this.ownExecutor = true;
    }

    BKDistributedLogManager(String name,
                            DistributedLogConfiguration conf,
                            URI uri,
                            ZooKeeperClientBuilder writerZKCBuilder,
                            ZooKeeperClientBuilder readerZKCBuilder,
                            ZooKeeperClient zkcForWriterBKC,
                            ZooKeeperClient zkcForReaderBKC,
                            BookKeeperClientBuilder writerBKCBuilder,
                            BookKeeperClientBuilder readerBKCBuilder,
                            ScheduledExecutorService executorService,
                            ScheduledExecutorService readAheadExecutor,
                            OrderedSafeExecutor lockStateExecutor,
                            ClientSocketChannelFactory channelFactory,
                            HashedWheelTimer requestTimer,
                            ReadAheadExceptionsLogger readAheadExceptionsLogger,
                            StatsLogger statsLogger) throws IOException {
        super(name, conf, uri, writerZKCBuilder, readerZKCBuilder, statsLogger);
        Preconditions.checkNotNull(readAheadExceptionsLogger, "No ReadAhead Stats Logger Provided.");
        this.conf = conf;
        this.executorService = executorService;
        this.lockStateExecutor = lockStateExecutor;
        this.readAheadExecutor = null == readAheadExecutor ? executorService : readAheadExecutor;
        this.statsLogger = statsLogger;
        this.ownExecutor = false;
        this.pendingReaders = new PendingReaders();

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

        // Stats
        StatsLogger handlerStatsLogger = statsLogger.scope("handlers");
        this.createWriteHandlerStats = handlerStatsLogger.getOpStatsLogger("create_write_handler");
        this.alertStatsLogger = new AlertStatsLogger(statsLogger, name);
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
    ExecutorServiceFuturePool getReaderFuturePool() {
        return this.readerFuturePool;
    }

    @Override
    public synchronized List<LogSegmentLedgerMetadata> getLogSegments() throws IOException {
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

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(PartitionId partition) throws IOException {
        return createReadLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionWriteHandler createWriteLedgerHandler(PartitionId partition) throws IOException {
        return createWriteLedgerHandler(partition.toString());
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier) throws IOException {
        return createReadLedgerHandler(streamIdentifier, false);
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier, boolean isHandleForReading)
            throws IOException {
        return createReadLedgerHandler(streamIdentifier, getLockStateExecutor(true), null, isHandleForReading);
    }

    synchronized public BKLogPartitionReadHandler createReadLedgerHandler(String streamIdentifier,
                                                                          OrderedSafeExecutor lockExecutor,
                                                                          AsyncNotification notification,
                                                                          boolean isHandleForReading) throws IOException {
        return new BKLogPartitionReadHandler(name, streamIdentifier, conf, uri,
                readerZKCBuilder, readerBKCBuilder, executorService, lockExecutor, readAheadExecutor,
                alertStatsLogger, readAheadExceptionsLogger, statsLogger, clientId, notification, isHandleForReading);
    }

    public BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier) throws IOException {
        return createWriteLedgerHandler(streamIdentifier, null, null);
    }

    BKLogPartitionWriteHandler createWriteLedgerHandler(String streamIdentifier,
                                                        FuturePool orderedFuturePool,
                                                        ExecutorService metadataExecutor)
            throws IOException {
        Stopwatch stopwatch = new Stopwatch().start();
        boolean success = false;
        try {
            BKLogPartitionWriteHandler handler = doCreateWriteLedgerHandler(streamIdentifier, orderedFuturePool, metadataExecutor);
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
                                                                   FuturePool orderedFuturePool,
                                                                   ExecutorService metadataExecutor)
            throws IOException {
        BKLogPartitionWriteHandler writeHandler =
            BKLogPartitionWriteHandler.createBKLogPartitionWriteHandler(name, streamIdentifier, conf, uri,
                writerZKCBuilder, writerBKCBuilder, executorService, orderedFuturePool, metadataExecutor,
                getLockStateExecutor(true), ledgerAllocator, statsLogger, clientId, regionId);
        PermitManager manager = getLogSegmentRollingPermitManager();
        if (manager instanceof Watcher) {
            writeHandler.register((Watcher) manager);
        }
        return writeHandler;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    int getRegionId() {
        return regionId;
    }

    void setRegionId(int regionId) {
        this.regionId = regionId;
    }

    public synchronized void setLedgerAllocator(LedgerAllocator allocator) {
        this.ledgerAllocator = allocator;
    }

    PermitManager getLogSegmentRollingPermitManager() {
        return logSegmentRollingPermitManager;
    }

    void setLogSegmentRollingPermitManager(PermitManager manager) {
        this.logSegmentRollingPermitManager = manager;
    }

    @VisibleForTesting
    synchronized void setMetadataExecutor(ExecutorService service) {
        this.metadataExecutor = service;
    }

    /**
     * Check if an end of stream marker was added to the stream for the partition
     * A stream with an end of stream marker cannot be appended to
     *
     * @return true if the marker was added to the stream, false otherwise
     */
    @Override
    public boolean isEndOfStreamMarked(PartitionId partition) throws IOException {
        throw new NotYetImplementedException("isEndOfStreamMarked for partitioned streams");
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
    public PartitionAwareLogWriter startLogSegment() throws IOException {
        checkClosedOrInError("startLogSegment");
        return new BKPartitionAwareLogWriter(conf, this);
    }

    /**
     * Begin writing to the log stream identified by the name
     *
     * @return the writer interface to generate log records
     */
    @Override
    public synchronized BKUnPartitionedSyncLogWriter startLogSegmentNonPartitioned() throws IOException {
        checkClosedOrInError("startLogSegmentNonPartitioned");
        return new BKUnPartitionedSyncLogWriter(conf, this);
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
            ExecutorService executorService = null;
            if (conf.getRecoverLogSegmentsInBackground()) {
                if (null == metadataExecutor) {
                    metadataExecutor = new ThreadPoolExecutor(0, 1, 60, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(),
                            new ThreadFactoryBuilder().setNameFormat("BKALW-" + name + "-metadata-executor-%d").build());
                }
                executorService = metadataExecutor;
            }

            // proactively recover incomplete logsegments for async log writer
            writer = new BKUnPartitionedAsyncLogWriter(conf, this, orderedFuturePool, executorService);
        }
        return writer.recover();
    }

    /**
     * Get the input stream starting with fromTxnId for the specified log
     *
     * @param partition – the partition (stream) within the log to read from
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public LogReader getInputStream(PartitionId partition, long fromTxnId)
        throws IOException {
        return getInputStreamInternal(partition.toString(), fromTxnId);
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
    public LogReader getInputStream(PartitionId partition, DLSN fromDLSN) throws IOException {
        return getInputStreamInternal(partition.toString(), fromDLSN);
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
        return new BKAsyncLogReaderDLSN(this, executorService, getLockStateExecutor(true),
                                        conf.getUnpartitionedStreamName(), fromDLSN,
                                        statsLogger, false /* lock stream */);
    }

    /**
     * Note the lock here is a sort of elective exclusive lock. I.e. acquiring this lock will only prevent other
     * people who try to acquire the lock from reading from the stream. Normal readers (and writers) will not be
     * blocked.
     */
    @Override
    public Future<AsyncLogReader> getAsyncLogReaderWithLock(final DLSN fromDLSN) {
        initializeFuturePool(false);
        /**
         * the #lockStream() call inside BKAsyncLogReaderDLSN is a blocking call. so we have to wrap
         * the construction of BKAsyncLogReaderDLSN into a future pool to not block caller thread.
         *
         * TODO: will make it clearer as a chaining of two actions after making lockStream pure asynchronous
         */
        return readerFuturePool.apply(new ExceptionalFunction0<BKAsyncLogReaderDLSN>() {
            @Override
            public BKAsyncLogReaderDLSN applyE() throws Throwable {
                // We don't need to use the same ordered executor for all locks, but the locks are unrelated, so it
                // also doesn't matter whether we do.
                BKAsyncLogReaderDLSN reader = new BKAsyncLogReaderDLSN(
                        BKDistributedLogManager.this, executorService,
                        getLockStateExecutor(true), conf.getUnpartitionedStreamName(),
                        fromDLSN, statsLogger, true /* lock stream */);
                // Since we're not returning the reader yet, we own the reader for now (ex. if something goes wrong
                // we'll have to clean up).
                pendingReaders.add(reader);
                return reader;
            }
        }).flatMap(new Function<BKAsyncLogReaderDLSN, Future<AsyncLogReader>>() {
            @Override
            public Future<AsyncLogReader> apply(final BKAsyncLogReaderDLSN reader) {
                return reader.getLockAcquireFuture().map(new Function<Void, AsyncLogReader>() {
                    @Override
                    public AsyncLogReader apply(Void complete) {
                        return reader;
                    }
                }).addEventListener(new FutureEventListener<AsyncLogReader>() {
                    @Override
                    public void onSuccess(AsyncLogReader r) {
                        pendingReaders.remove(reader);
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        try {
                            pendingReaders.remove(reader);
                            reader.close();
                        } catch (IOException ioe) {
                            LOG.error("failed to close reader on failure");
                        }
                    }
                });
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

    /**
     * Get the last log record no later than the specified transactionId
     *
     * @param partition – the partition within the log stream to read from
     * @param thresholdTxId - the threshold transaction id
     * @return the last log record before a given transactionId
     * @throws IOException if a stream cannot be found.
     */
    @Override
    public long getTxIdNotLaterThan(PartitionId partition, long thresholdTxId) throws IOException {
        return getTxIdNotLaterThanInternal(partition.toString(), thresholdTxId);
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
     * @param partition – the partition within the log stream to read from
     * @return the last log record in the stream
     * @throws java.io.IOException if a stream cannot be found.
     */
    @Override
    public LogRecordWithDLSN getLastLogRecord(PartitionId partition) throws IOException {
        return getLastLogRecordInternal(partition.toString());
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
    public long getFirstTxId(PartitionId partition) throws IOException {
        return getFirstTxIdInternal(partition.toString());
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
    public long getLastTxId(PartitionId partition) throws IOException {
        return getLastTxIdInternal(partition.toString(), false, false);
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
    public DLSN getLastDLSN(PartitionId partition) throws IOException {
        return getLastDLSNInternal(partition.toString(), false, false);
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

    private Future<LogRecordWithDLSN> getLastRecordAsyncInternal(final String streamIdentifier,
                                                                 final boolean recover,
                                                                 final boolean includeEndOfStream) {
        initializeFuturePool(false);
        return readerFuturePool.apply(new ExceptionalFunction0<BKLogPartitionReadHandler>() {
            @Override
            public BKLogPartitionReadHandler applyE() throws IOException {
                return createReadLedgerHandler(streamIdentifier);
            }
        }).flatMap(new Function<BKLogPartitionReadHandler, Future<LogRecordWithDLSN>>() {
            @Override
            public Future<LogRecordWithDLSN> apply(BKLogPartitionReadHandler ledgerHandler) {
                return ledgerHandler.getLastLogRecordAsync(recover, includeEndOfStream);
            }
        });
    }

    /**
     * Get Latest Transaction Id in the specified partition of the log
     *
     * @param partition - the partition within the log
     * @return latest transaction id
     */
    @Override
    public Future<Long> getLastTxIdAsync(PartitionId partition) {
        return getLastTxIdAsyncInternal(partition.toString());
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
        initializeFuturePool(false);
        return readerFuturePool.apply(new ExceptionalFunction0<BKLogPartitionReadHandler>() {
            @Override
            public BKLogPartitionReadHandler applyE() throws IOException {
                return createReadLedgerHandler(conf.getUnpartitionedStreamName());
            }
        }).flatMap(new Function<BKLogPartitionReadHandler, Future<LogRecordWithDLSN>>() {
            @Override
            public Future<LogRecordWithDLSN> apply(BKLogPartitionReadHandler ledgerHandler) {
                return ledgerHandler.asyncGetFirstLogRecord();
            }
        });
    }

    /**
     * Get Latest DLSN in the specified partition of the log
     *
     * @param partition - the partition within the log
     * @return latest transaction id
     */
    @Override
    public Future<DLSN> getLastDLSNAsync(PartitionId partition) {
        return getLastDLSNAsyncInternal(partition.toString());
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
     * Get the number of log records in the active portion of the stream for the
     * given partition
     * Any log segments that have already been truncated will not be included
     *
     * @param partition the partition within the log
     * @return number of log records
     * @throws IOException
     */
    @Override
    public long getLogRecordCount(PartitionId partition) throws IOException {
        return getLogRecordCountInternal(partition.toString());
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
        initializeFuturePool(false);
        return readerFuturePool.apply(new ExceptionalFunction0<BKLogPartitionReadHandler>() {
            @Override
            public BKLogPartitionReadHandler applyE() throws IOException {
                return createReadLedgerHandler(conf.getUnpartitionedStreamName());
            }
        }).flatMap(new Function<BKLogPartitionReadHandler, Future<Long>>() {
            @Override
            public Future<Long> apply(BKLogPartitionReadHandler ledgerHandler) {
                return ledgerHandler.asyncGetLogRecordCount(beginDLSN);
            }
        });
    }

    /**
     * Recover a specified partition within the log container
     *
     * @param partition – the partition within the log stream to delete
     * @throws IOException if the recovery fails
     */
    @Override
    public void recover(PartitionId partition) throws IOException {
        recoverInternal(partition.toString());
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
     * @param partition – the partition within the log stream to delete
     * @throws IOException if the deletion fails
     */
    @Override
    public void deletePartition(PartitionId partition) throws IOException {
        deletePartition(partition.toString());
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

        // Clean up executor state.
        if (ownExecutor) {
            SchedulerUtils.shutdownScheduler(executorService, 5000, TimeUnit.MILLISECONDS);
            LOG.info("Stopped BKDL executor service for {}.", name);

            if (executorService != readAheadExecutor) {
                SchedulerUtils.shutdownScheduler(readAheadExecutor, 5000, TimeUnit.MILLISECONDS);
                LOG.info("Stopped BKDL ReadAhead Executor Service for {}.", name);
            }

            SchedulerUtils.shutdownScheduler(getLockStateExecutor(false), 5000, TimeUnit.MILLISECONDS);
            LOG.info("Stopped BKDL Lock State Executor for {}.", name);
        } else {
            if (null != orderedFuturePool) {
                SchedulerUtils.shutdownScheduler(orderedFuturePool.executor(), 5000, TimeUnit.MILLISECONDS);
                LOG.info("Stopped Ordered Future Pool for {}.", name);
            }
            SchedulerUtils.shutdownScheduler(metadataExecutor, 5000, TimeUnit.MILLISECONDS);
            LOG.info("Stopped BKDL metadata executor for {}.", name);
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
            executorService.submit(task);
            return true;
        } catch (RejectedExecutionException ree) {
            LOG.error("Task {} is rejected : ", task, ree);
            return false;
        }
    }

    private void initializeFuturePool(boolean ordered) {
        // Note for orderedFuturePool:
        // Single Threaded Future Pool inherently preserves order by tasks one by one
        //
        if (ownExecutor) {
            // ownExecutor is a single threaded thread pool
            if (null == orderedFuturePool) {
                // Readers share the same future pool as the orderedFuturePool
                orderedFuturePool = new ExecutorServiceFuturePool(executorService);
                readerFuturePool = orderedFuturePool;
            }
        } else if (ordered && (null == orderedFuturePool)) {
            // When we are using a thread pool that was passed from the factory, we can use
            // the executor service
            orderedFuturePool = new ExecutorServiceFuturePool(Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("BKALW-" + name + "-executor-%d").build()));
        } else if (!ordered && (null == readerFuturePool)) {
            // readerFuturePool can just use the executor service that was configured with the DLM
            readerFuturePool = new ExecutorServiceFuturePool(executorService);
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
    public SubscriptionStateStore getSubscriptionStateStore(String subscriberId) {
        return getSubscriptionStateStoreInternal(conf.getUnpartitionedStreamName(), subscriberId);
    }

    /**
     * Get the subscription state storage provided by the distributed log manager
     *
     * @param partition - the partition within the log stream
     * @param subscriberId - Application specific Id associated with the subscriber
     * @return Subscription state store
     */
    @Override
    public SubscriptionStateStore getSubscriptionStateStore(PartitionId partition, String subscriberId) {
        return getSubscriptionStateStoreInternal(partition.toString(), subscriberId);
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
            String.format("%s/subscribers/%s", getPartitionPath(uri, name, streamIdentifier), subscriberId));
    }
}
