package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.IdleReaderException;
import com.twitter.distributedlog.exceptions.ReadCancelledException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.injector.AsyncFailureInjector;
import com.twitter.distributedlog.injector.AsyncRandomFailureInjector;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Throw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.runtime.AbstractFunction1;

/**
 * BookKeeper based {@link AsyncLogReader} implementation.
 *
 * <h3>Metrics</h3>
 * All the metrics are exposed under `async_reader`.
 * <ul>
 * <li> `async_reader`/future_set: opstats. time spent on satisfying futures of read requests.
 * if it is high, it means that the caller takes time on processing the result of read requests.
 * The side effect is blocking consequent reads.
 * <li> `async_reader`/schedule: opstats. time spent on scheduling next reads.
 * <li> `async_reader`/background_read: opstats. time spent on background reads.
 * <li> `async_reader`/read_next_exec: opstats. time spent on executing {@link #readNext()}.
 * <li> `async_reader`/time_between_read_next: opstats. time spent on between two consequent {@link #readNext()}.
 * if it is high, it means that the caller is slowing down on calling {@link #readNext()}.
 * <li> `async_reader`/delay_until_promise_satisfied: opstats. total latency for the read requests.
 * <li> `async_reader`/idle_reader_error: counter. the number idle reader errors.
 * </ul>
 */
class BKAsyncLogReaderDLSN implements ZooKeeperClient.ZooKeeperSessionExpireNotifier, AsyncLogReader, Runnable, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogReaderDLSN.class);

    private static final Function1<List<LogRecordWithDLSN>, LogRecordWithDLSN> READ_NEXT_MAP_FUNCTION =
            new AbstractFunction1<List<LogRecordWithDLSN>, LogRecordWithDLSN>() {
                @Override
                public LogRecordWithDLSN apply(List<LogRecordWithDLSN> records) {
                    return records.get(0);
                }
            };

    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogReadHandler bkLedgerManager;
    private Watcher sessionExpireWatcher = null;
    private final AtomicReference<Throwable> lastException = new AtomicReference<Throwable>();
    private final ScheduledExecutorService executorService;
    private final ConcurrentLinkedQueue<PendingReadRequest> pendingRequests = new ConcurrentLinkedQueue<PendingReadRequest>();
    private final AtomicLong scheduleCount = new AtomicLong(0);
    final private Stopwatch scheduleDelayStopwatch;
    final private Stopwatch readNextDelayStopwatch;
    private DLSN startDLSN;
    private boolean readAheadStarted = false;
    private int lastPosition = 0;
    private final boolean positionGapDetectionEnabled;
    private final int idleErrorThresholdMillis;
    private final ScheduledFuture<?> idleReaderTimeoutTask;
    private ScheduledFuture<?> backgroundScheduleTask = null;

    protected boolean closed = false;

    private boolean lockStream = false;

    private boolean disableReadAheadZKNotification = false;

    private final boolean returnEndOfStreamRecord;

    private final Runnable BACKGROUND_READ_SCHEDULER = new Runnable() {
        @Override
        public void run() {
            synchronized (scheduleCount) {
                backgroundScheduleTask = null;
            }
            scheduleBackgroundRead();
        }
    };

    // Failure Injector
    private final AsyncFailureInjector failureInjector;
    private boolean disableProcessingReadRequests = false;

    // Stats
    private final OpStatsLogger readNextExecTime;
    private final OpStatsLogger delayUntilPromiseSatisfied;
    private final OpStatsLogger timeBetweenReadNexts;
    private final OpStatsLogger futureSetLatency;
    private final OpStatsLogger scheduleLatency;
    private final OpStatsLogger backgroundReaderRunTime;
    private final Counter idleReaderCheckCount;
    private final Counter idleReaderCheckIdleReadRequestCount;
    private final Counter idleReaderCheckIdleReadAheadCount;
    private final Counter idleReaderError;

    private class PendingReadRequest {
        private final Stopwatch enqueueTime;
        private final int numEntries;
        private final List<LogRecordWithDLSN> records;
        private final Promise<List<LogRecordWithDLSN>> promise;
        private final long deadlineTime;
        private final TimeUnit deadlineTimeUnit;

        PendingReadRequest(int numEntries,
                           long deadlineTime,
                           TimeUnit deadlineTimeUnit) {
            this.numEntries = numEntries;
            this.enqueueTime = Stopwatch.createStarted();
            // optimize the space usage for single read.
            if (numEntries == 1) {
                this.records = new ArrayList<LogRecordWithDLSN>(1);
            } else {
                this.records = new ArrayList<LogRecordWithDLSN>();
            }
            this.promise = new Promise<List<LogRecordWithDLSN>>();
            this.deadlineTime = deadlineTime;
            this.deadlineTimeUnit = deadlineTimeUnit;
        }

        Promise<List<LogRecordWithDLSN>> getPromise() {
            return promise;
        }

        long elapsedSinceEnqueue(TimeUnit timeUnit) {
            return enqueueTime.elapsed(timeUnit);
        }

        void setException(Throwable throwable) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            if (promise.updateIfEmpty(new Throw<List<LogRecordWithDLSN>>(throwable))) {
                futureSetLatency.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                delayUntilPromiseSatisfied.registerFailedEvent(enqueueTime.elapsed(TimeUnit.MICROSECONDS));
            }
        }

        boolean hasReadRecords() {
            return records.size() > 0;
        }

        boolean hasReadEnoughRecords() {
            return records.size() >= numEntries;
        }

        long getRemainingWaitTime() {
            if (deadlineTime <= 0L) {
                return 0L;
            }
            return deadlineTime - elapsedSinceEnqueue(deadlineTimeUnit);
        }

        void addRecord(LogRecordWithDLSN record) {
            records.add(record);
        }

        void complete() {
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} : Satisfied promise with {} records", bkLedgerManager.getFullyQualifiedName(), records.size());
            }
            delayUntilPromiseSatisfied.registerSuccessfulEvent(enqueueTime.stop().elapsed(TimeUnit.MICROSECONDS));
            Stopwatch stopwatch = Stopwatch.createStarted();
            promise.setValue(records);
            futureSetLatency.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
        }
    }

    BKAsyncLogReaderDLSN(BKDistributedLogManager bkdlm,
                         ScheduledExecutorService executorService,
                         OrderedScheduler lockStateExecutor,
                         DLSN startDLSN,
                         Optional<String> subscriberId,
                         boolean returnEndOfStreamRecord,
                         StatsLogger statsLogger) {
        this.bkDistributedLogManager = bkdlm;
        this.executorService = executorService;
        this.bkLedgerManager = bkDistributedLogManager.createReadHandler(subscriberId,
                lockStateExecutor, this, true);
        sessionExpireWatcher = this.bkLedgerManager.registerExpirationHandler(this);
        LOG.debug("Starting async reader at {}", startDLSN);
        this.startDLSN = startDLSN;
        this.scheduleDelayStopwatch = Stopwatch.createUnstarted();
        this.readNextDelayStopwatch = Stopwatch.createStarted();
        this.positionGapDetectionEnabled = bkdlm.getConf().getPositionGapDetectionEnabled();
        this.idleErrorThresholdMillis = bkdlm.getConf().getReaderIdleErrorThresholdMillis();
        this.returnEndOfStreamRecord = returnEndOfStreamRecord;

        // Failure Injection
        this.failureInjector = AsyncRandomFailureInjector.newBuilder()
                .injectDelays(bkdlm.getConf().getEIInjectReadAheadDelay(),
                              bkdlm.getConf().getEIInjectReadAheadDelayPercent(),
                              bkdlm.getConf().getEIInjectMaxReadAheadDelayMs())
                .injectErrors(false, 10)
                .injectStops(bkdlm.getConf().getEIInjectReadAheadStall(), 10)
                .injectCorruption(bkdlm.getConf().getEIInjectReadAheadBrokenEntries())
                .build();

        // Stats
        StatsLogger asyncReaderStatsLogger = statsLogger.scope("async_reader");
        futureSetLatency = asyncReaderStatsLogger.getOpStatsLogger("future_set");
        scheduleLatency = asyncReaderStatsLogger.getOpStatsLogger("schedule");
        backgroundReaderRunTime = asyncReaderStatsLogger.getOpStatsLogger("background_read");
        readNextExecTime = asyncReaderStatsLogger.getOpStatsLogger("read_next_exec");
        timeBetweenReadNexts = asyncReaderStatsLogger.getOpStatsLogger("time_between_read_next");
        delayUntilPromiseSatisfied = asyncReaderStatsLogger.getOpStatsLogger("delay_until_promise_satisfied");
        idleReaderError = asyncReaderStatsLogger.getCounter("idle_reader_error");
        idleReaderCheckCount = asyncReaderStatsLogger.getCounter("idle_reader_check_total");
        idleReaderCheckIdleReadRequestCount = asyncReaderStatsLogger.getCounter("idle_reader_check_idle_read_requests");
        idleReaderCheckIdleReadAheadCount = asyncReaderStatsLogger.getCounter("idle_reader_check_idle_readahead");

        // Lock the stream if requested. The lock will be released when the reader is closed.
        this.lockStream = false;
        this.idleReaderTimeoutTask = scheduleIdleReaderTaskIfNecessary();
    }

    @Override
    public void notifySessionExpired() {
        // ZK Session notification is an indication to check if this has resulted in a fatal error
        // of the underlying reader, in itself this reader doesnt error out unless the underlying
        // reader has hit an error
        scheduleBackgroundRead();
    }

    private ScheduledFuture<?> scheduleIdleReaderTaskIfNecessary() {
        if (idleErrorThresholdMillis < Integer.MAX_VALUE) {
            // Dont run the task more than once every seconds (for sanity)
            long period = Math.max(idleErrorThresholdMillis / 10, 1000);
            // Except when idle reader threshold is less than a second (tests?)
            period = Math.min(period, idleErrorThresholdMillis / 5);

            return executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    PendingReadRequest nextRequest = pendingRequests.peek();

                    idleReaderCheckCount.inc();
                    if (null == nextRequest) {
                        return;
                    }

                    idleReaderCheckIdleReadRequestCount.inc();
                    if (nextRequest.elapsedSinceEnqueue(TimeUnit.MILLISECONDS) < idleErrorThresholdMillis) {
                        return;
                    }

                    ReadAheadCache cache = bkLedgerManager.getReadAheadCache();

                    // read request has been idle
                    //   - cache has records but read request are idle,
                    //     that means notification was missed between readahead and reader.
                    //   - cache is empty and readahead is idle (no records added for a long time)
                    idleReaderCheckIdleReadAheadCount.inc();
                    if (cache.getNumCachedRecords() <= 0
                            && !cache.isReadAheadIdle(idleErrorThresholdMillis, TimeUnit.MILLISECONDS)) {
                        return;
                    }

                    idleReaderError.inc();
                    IdleReaderException ire = new IdleReaderException("Reader on stream "
                            + bkLedgerManager.getFullyQualifiedName()
                            + " is idle for " + idleErrorThresholdMillis +" ms");
                    setLastException(ire);
                    // cancel all pending reads directly rather than notifying on error
                    // because idle reader could happen on idle read requests that usually means something wrong
                    // in scheduling reads
                    cancelAllPendingReads(ire);
                }
            }, period, period, TimeUnit.MILLISECONDS);
        }

        return null;
    }

    protected synchronized void setStartDLSN(DLSN fromDLSN) throws UnexpectedException {
        if (readAheadStarted) {
            throw new UnexpectedException("Could't reset from dlsn after reader already starts reading.");
        }
        startDLSN = fromDLSN;
    }

    @VisibleForTesting
    public synchronized DLSN getStartDLSN() {
        return startDLSN;
    }

    public Future<Void> lockStream() {
        this.lockStream = true;
        return bkLedgerManager.lockStream();
    }

    private boolean checkClosedOrInError(String operation) {
        if (null == lastException.get()) {
            try {
                if (null != bkLedgerManager && null != bkLedgerManager.readAheadWorker) {
                    bkLedgerManager.readAheadWorker.checkClosedOrInError();
                }

                bkDistributedLogManager.checkClosedOrInError(operation);
            } catch (IOException exc) {
                setLastException(exc);
            }
        }

        if (lockStream) {
            try {
                bkLedgerManager.checkReadLock();
            } catch (IOException ex) {
                setLastException(ex);
            }
        }

        if (null != lastException.get()) {
            LOG.trace("Cancelling pending reads");
            cancelAllPendingReads(lastException.get());
            return true;
        }

        return false;
    }

    private void setLastException(IOException exc) {
        lastException.compareAndSet(null, exc);
    }

    @Override
    public String getStreamName() {
        return bkDistributedLogManager.getStreamName();
    }

    /**
     * @return A promise that when satisfied will contain the Log Record with its DLSN.
     */
    @Override
    public synchronized Future<LogRecordWithDLSN> readNext() {
        return readInternal(1, 0, TimeUnit.MILLISECONDS).map(READ_NEXT_MAP_FUNCTION);
    }

    public synchronized Future<List<LogRecordWithDLSN>> readBulk(int numEntries) {
        return readInternal(numEntries, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized Future<List<LogRecordWithDLSN>> readBulk(int numEntries,
                                                                 long waitTime,
                                                                 TimeUnit timeUnit) {
        return readInternal(numEntries, waitTime, timeUnit);
    }

    /**
     * Read up to <i>numEntries</i> entries. The future will be satisfied when any number of entries are
     * ready (1 to <i>numEntries</i>).
     *
     * @param numEntries
     *          num entries to read
     * @return A promise that satisfied with a non-empty list of log records with their DLSN.
     */
    private synchronized Future<List<LogRecordWithDLSN>> readInternal(int numEntries,
                                                                      long deadlineTime,
                                                                      TimeUnit deadlineTimeUnit) {
        timeBetweenReadNexts.registerSuccessfulEvent(readNextDelayStopwatch.elapsed(TimeUnit.MICROSECONDS));
        readNextDelayStopwatch.reset().start();
        final PendingReadRequest readRequest = new PendingReadRequest(numEntries, deadlineTime, deadlineTimeUnit);

        if (!readAheadStarted) {
            bkLedgerManager.checkLogStreamExistsAsync().addEventListener(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    try {
                        bkLedgerManager.startReadAhead(
                                new LedgerReadPosition(getStartDLSN()),
                                failureInjector);
                        if (disableReadAheadZKNotification) {
                            bkLedgerManager.disableReadAheadZKNotification();
                        }
                    } catch (Exception exc) {
                        setLastException(new IOException(exc));
                        notifyOnError();
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    if (cause instanceof IOException) {
                        setLastException((IOException)cause);
                    } else {
                        setLastException(new IOException(cause));
                    }
                    notifyOnError();
                }
            });
            readAheadStarted = true;
        }

        if (checkClosedOrInError("readNext")) {
            readRequest.setException(lastException.get());
        } else {
            boolean queueEmpty = pendingRequests.isEmpty();
            pendingRequests.add(readRequest);

            if (queueEmpty) {
                scheduleBackgroundRead();
            }
        }

        readNextExecTime.registerSuccessfulEvent(readNextDelayStopwatch.elapsed(TimeUnit.MICROSECONDS));
        readNextDelayStopwatch.reset().start();

        return readRequest.getPromise();
    }

    public synchronized void scheduleBackgroundRead() {
        // if the reader is already closed, we don't need to schedule background read again.
        if (closed) {
            return;
        }

        long prevCount = scheduleCount.getAndIncrement();
        if (0 == prevCount) {
            scheduleDelayStopwatch.reset().start();
            executorService.submit(this);
        }
    }

    @Override
    public void close() {
        // Cancel the idle reader timeout task, interrupting if necessary
        ReadCancelledException exception;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            exception = new ReadCancelledException(bkLedgerManager.getFullyQualifiedName(), "Reader was closed");
            setLastException(exception);
        }

        // Do this after we have checked that the reader was not previously closed
        try {
            if (null != idleReaderTimeoutTask) {
                idleReaderTimeoutTask.cancel(true);
            }
        } catch (Exception exc) {
            LOG.info("{}: Failed to cancel the background idle reader timeout task", bkLedgerManager.getFullyQualifiedName());
        }

        synchronized (scheduleCount) {
            if (null != backgroundScheduleTask) {
                backgroundScheduleTask.cancel(true);
            }
        }

        cancelAllPendingReads(exception);

        bkLedgerManager.unregister(sessionExpireWatcher);

        // Also releases the read lock, if acquired.
        bkLedgerManager.close();
    }

    private void cancelAllPendingReads(Throwable throwExc) {
        for (PendingReadRequest promise : pendingRequests) {
            promise.setException(throwExc);
        }
        pendingRequests.clear();
    }

    @Override
    public void run() {
        synchronized(scheduleCount) {
            if (scheduleDelayStopwatch.isRunning()) {
                scheduleLatency.registerSuccessfulEvent(scheduleDelayStopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }

            Stopwatch runTime = Stopwatch.createStarted();
            int iterations = 0;
            long scheduleCountLocal = scheduleCount.get();
            LOG.debug("{}: Scheduled Background Reader", bkLedgerManager.getFullyQualifiedName());
            while(true) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{}: Executing Iteration: {}", bkLedgerManager.getFullyQualifiedName(), iterations++);
                }

                PendingReadRequest nextRequest = null;
                synchronized(this) {
                    nextRequest = pendingRequests.peek();

                    // Queue is empty, nothing to read, return
                    if (null == nextRequest) {
                        LOG.trace("{}: Queue Empty waiting for Input", bkLedgerManager.getFullyQualifiedName());
                        scheduleCount.set(0);
                        backgroundReaderRunTime.registerSuccessfulEvent(runTime.stop().elapsed(TimeUnit.MICROSECONDS));
                        return;
                    }
                }

                if (disableProcessingReadRequests) {
                    LOG.info("Reader of {} is forced to stop processing read requests", bkLedgerManager.getFullyQualifiedName());
                    return;
                }

                // If the oldest pending promise is interrupted then we must mark
                // the reader in error and abort all pending reads since we dont
                // know the last consumed read
                if (null == lastException.get()) {
                    if (nextRequest.getPromise().isInterrupted().isDefined()) {
                        setLastException(new DLInterruptedException("Interrupted on reading " + bkLedgerManager.getFullyQualifiedName() + " : ",
                                nextRequest.getPromise().isInterrupted().get()));
                    }
                }

                if (checkClosedOrInError("readNext")) {
                    if (!(lastException.get().getCause() instanceof LogNotFoundException)) {
                        LOG.warn("{}: Exception", bkLedgerManager.getFullyQualifiedName(), lastException.get());
                    }
                    backgroundReaderRunTime.registerFailedEvent(runTime.stop().elapsed(TimeUnit.MICROSECONDS));
                    return;
                }

                try {
                    // Fail 10% of the requests when asked to simulate errors
                    if (failureInjector.shouldInjectErrors()) {
                        throw new IOException("Reader Simulated Exception");
                    }
                    LogRecordWithDLSN record;
                    while (!nextRequest.hasReadEnoughRecords()) {
                        // read single record
                        do {
                            record = bkLedgerManager.getNextReadAheadRecord();
                        } while (null != record && (record.isControl() || (record.getDlsn().compareTo(getStartDLSN()) < 0)));
                        if (null == record) {
                            break;
                        } else {
                            if (record.isEndOfStream() && !returnEndOfStreamRecord) {
                                setLastException(new EndOfStreamException("End of Stream Reached for "
                                        + bkLedgerManager.getFullyQualifiedName()));
                                break;
                            }

                            // gap detection
                            if (recordPositionsContainsGap(record, lastPosition)) {
                                bkDistributedLogManager.raiseAlert("Gap detected between records at dlsn = {}", record.getDlsn());
                                if (positionGapDetectionEnabled) {
                                    throw new DLIllegalStateException("Gap detected between records at dlsn = " + record.getDlsn());
                                }
                            }
                            lastPosition = record.getPositionWithinLogSegment();

                            nextRequest.addRecord(record);
                        }
                    };
                } catch (IOException exc) {
                    setLastException(exc);
                    if (!(exc instanceof LogNotFoundException)) {
                        LOG.warn("{} : read with skip Exception", bkLedgerManager.getFullyQualifiedName(), lastException.get());
                    }
                    continue;
                }

                if (nextRequest.hasReadRecords()) {
                    long remainingWaitTime = nextRequest.getRemainingWaitTime();
                    if (remainingWaitTime > 0 && !nextRequest.hasReadEnoughRecords()) {
                        backgroundReaderRunTime.registerSuccessfulEvent(runTime.stop().elapsed(TimeUnit.MICROSECONDS));
                        scheduleDelayStopwatch.reset().start();
                        scheduleCount.set(0);
                        // the request could still wait for more records
                        backgroundScheduleTask = executorService.schedule(BACKGROUND_READ_SCHEDULER, remainingWaitTime, nextRequest.deadlineTimeUnit);
                        return;
                    }

                    PendingReadRequest request = pendingRequests.poll();
                    if (null != request && nextRequest == request) {
                        request.complete();
                        if (null != backgroundScheduleTask) {
                            backgroundScheduleTask.cancel(true);
                            backgroundScheduleTask = null;
                        }
                    } else {
                        DLIllegalStateException ise = new DLIllegalStateException("Unexpected condition at dlsn = "
                                + nextRequest.records.get(0).getDlsn());
                        nextRequest.setException(ise);
                        if (null != request) {
                            request.setException(ise);
                        }
                        // We should never get here as we should have exited the loop if
                        // pendingRequests were empty
                        bkDistributedLogManager.raiseAlert("Unexpected condition at dlsn = {}",
                                nextRequest.records.get(0).getDlsn());
                        setLastException(ise);
                    }
                } else {
                    if (0 == scheduleCountLocal) {
                        LOG.trace("Schedule count dropping to zero", lastException.get());
                        backgroundReaderRunTime.registerSuccessfulEvent(runTime.stop().elapsed(TimeUnit.MICROSECONDS));
                        return;
                    }
                    scheduleCountLocal = scheduleCount.decrementAndGet();
                }
            }
        }
    }

    private boolean recordPositionsContainsGap(LogRecordWithDLSN record, long lastPosition) {
        final boolean firstLogRecord = (1 == record.getPositionWithinLogSegment());
        final boolean endOfStreamRecord = record.isEndOfStream();
        final boolean emptyLogSegment = (0 == lastPosition);
        final boolean positionIncreasedByOne = (record.getPositionWithinLogSegment() == (lastPosition + 1));

        return !firstLogRecord && !endOfStreamRecord && !emptyLogSegment &&
               !positionIncreasedByOne;
    }

    /**
     * Triggered when the background activity encounters an exception
     */
    @Override
    public void notifyOnError() {
        scheduleBackgroundRead();
    }

    /**
     * Triggered when the background activity completes an operation
     */
    @Override
    public void notifyOnOperationComplete() {
        scheduleBackgroundRead();
    }

    @VisibleForTesting
    void simulateErrors() {
        failureInjector.injectErrors(true);
    }

    @VisibleForTesting
    synchronized void disableReadAheadZKNotification() {
        disableReadAheadZKNotification = true;
        bkLedgerManager.disableReadAheadZKNotification();
    }

    @VisibleForTesting
    synchronized void disableProcessingReadRequests() {
        disableProcessingReadRequests = true;
    }
}

