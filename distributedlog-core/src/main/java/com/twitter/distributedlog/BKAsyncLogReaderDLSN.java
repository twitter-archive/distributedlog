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
import org.apache.bookkeeper.util.OrderedSafeExecutor;
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
    private AtomicReference<Throwable> lastException = new AtomicReference<Throwable>();
    private ScheduledExecutorService executorService;
    private ConcurrentLinkedQueue<PendingReadRequest> pendingRequests = new ConcurrentLinkedQueue<PendingReadRequest>();
    private AtomicLong scheduleCount = new AtomicLong(0);
    private boolean simulateErrors = false;
    final private Stopwatch scheduleDelayStopwatch;
    final private Stopwatch readNextDelayStopwatch;
    private DLSN startDLSN;
    private boolean readAheadStarted = false;
    private int lastPosition = 0;
    private final boolean positionGapDetectionEnabled;
    private final int idleErrorThresholdMillis;
    private final ScheduledFuture<?> idleReaderTimeoutTask;

    protected boolean closed = false;

    private boolean lockStream = false;

    private boolean disableReadAheadZKNotification = false;

    private final boolean returnEndOfStreamRecord;

    // Stats
    private final OpStatsLogger readNextExecTime;
    private final OpStatsLogger delayUntilPromiseSatisfied;
    private final OpStatsLogger timeBetweenReadNexts;
    private final OpStatsLogger futureSetLatency;
    private final OpStatsLogger scheduleLatency;
    private final OpStatsLogger backgroundReaderRunTime;
    private final Counter idleReaderError;

    private class PendingReadRequest {
        private final Stopwatch enqueueTime;
        private final int numEntries;
        private final Promise<List<LogRecordWithDLSN>> promise;

        PendingReadRequest(int numEntries) {
            this.numEntries = numEntries;
            this.enqueueTime = Stopwatch.createStarted();
            this.promise = new Promise<List<LogRecordWithDLSN>>();
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

        void setValue(List<LogRecordWithDLSN> records) {
            delayUntilPromiseSatisfied.registerSuccessfulEvent(enqueueTime.stop().elapsed(TimeUnit.MICROSECONDS));
            Stopwatch stopwatch = Stopwatch.createStarted();
            promise.setValue(records);
            futureSetLatency.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
        }
    }

    BKAsyncLogReaderDLSN(BKDistributedLogManager bkdlm,
                         ScheduledExecutorService executorService,
                         OrderedSafeExecutor lockStateExecutor,
                         String streamIdentifier,
                         DLSN startDLSN,
                         Optional<String> subscriberId,
                         boolean returnEndOfStreamRecord,
                         StatsLogger statsLogger) {
        this.bkDistributedLogManager = bkdlm;
        this.executorService = executorService;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier, subscriberId,
                lockStateExecutor, this, true);
        sessionExpireWatcher = this.bkLedgerManager.registerExpirationHandler(this);
        LOG.debug("Starting async reader at {}", startDLSN);
        this.startDLSN = startDLSN;
        this.scheduleDelayStopwatch = Stopwatch.createUnstarted();
        this.readNextDelayStopwatch = Stopwatch.createStarted();
        this.positionGapDetectionEnabled = bkdlm.getConf().getPositionGapDetectionEnabled();
        this.idleErrorThresholdMillis = bkdlm.getConf().getReaderIdleErrorThresholdMillis();
        this.returnEndOfStreamRecord = returnEndOfStreamRecord;

        StatsLogger asyncReaderStatsLogger = statsLogger.scope("async_reader");
        futureSetLatency = asyncReaderStatsLogger.getOpStatsLogger("future_set");
        scheduleLatency = asyncReaderStatsLogger.getOpStatsLogger("schedule");
        backgroundReaderRunTime = asyncReaderStatsLogger.getOpStatsLogger("background_read");
        readNextExecTime = asyncReaderStatsLogger.getOpStatsLogger("read_next_exec");
        timeBetweenReadNexts = asyncReaderStatsLogger.getOpStatsLogger("time_between_read_next");
        delayUntilPromiseSatisfied = asyncReaderStatsLogger.getOpStatsLogger("delay_until_promise_satisfied");
        idleReaderError = asyncReaderStatsLogger.getCounter("idle_reader_error");

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
                    PendingReadRequest nextPromise = pendingRequests.peek();

                    if (null == nextPromise) {
                        return;
                    }

                    if (nextPromise.elapsedSinceEnqueue(TimeUnit.MILLISECONDS) < idleErrorThresholdMillis) {
                        return;
                    }

                    if (!bkLedgerManager.checkForReaderStall(idleErrorThresholdMillis, TimeUnit.MILLISECONDS)) {
                        return;
                    }

                    idleReaderError.inc();
                    setLastException(new IdleReaderException("Reader on stream" +
                        bkLedgerManager.getFullyQualifiedName()
                        + "is idle for " + idleErrorThresholdMillis +"ms"));
                    notifyOnError();
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
                if (null != bkLedgerManager) {
                    bkLedgerManager.checkClosedOrInError();
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
        return readInternal(1).map(READ_NEXT_MAP_FUNCTION);
    }

    public synchronized Future<List<LogRecordWithDLSN>> readBulk(int numEntries) {
        return readInternal(numEntries);
    }

    /**
     * Read up to <i>numEntries</i> entries. The future will be satisfied when any number of entries are
     * ready (1 to <i>numEntries</i>).
     *
     * @param numEntries
     *          num entries to read
     * @return A promise that satisfied with a non-empty list of log records with their DLSN.
     */
    private synchronized Future<List<LogRecordWithDLSN>> readInternal(int numEntries) {
        timeBetweenReadNexts.registerSuccessfulEvent(readNextDelayStopwatch.elapsed(TimeUnit.MICROSECONDS));
        readNextDelayStopwatch.reset().start();
        final PendingReadRequest readRequest = new PendingReadRequest(numEntries);

        if (!readAheadStarted) {
            bkLedgerManager.checkLogStreamExistsAsync().addEventListener(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    try {
                        bkLedgerManager.startReadAhead(new LedgerReadPosition(getStartDLSN()), simulateErrors);
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

                List<LogRecordWithDLSN> records;
                // optimize the space usage for single read.
                if (1 == nextRequest.numEntries) {
                    records = new ArrayList<LogRecordWithDLSN>(1);
                } else {
                    records = new ArrayList<LogRecordWithDLSN>();
                }
                try {
                    // Fail 10% of the requests when asked to simulate errors
                    if (simulateErrors && Utils.randomPercent(10)) {
                        throw new IOException("Reader Simulated Exception");
                    }
                    LogRecordWithDLSN record;
                    int numReads = 0;
                    do {
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
                            records.add(record);
                            ++numReads;
                        }
                    } while (numReads < nextRequest.numEntries);
                } catch (IOException exc) {
                    setLastException(exc);
                    if (!(exc instanceof LogNotFoundException)) {
                        LOG.warn("{} : read with skip Exception", bkLedgerManager.getFullyQualifiedName(), lastException.get());
                    }
                    continue;
                }

                if (!records.isEmpty()) {
                    // Verify that the count is contiguous and monotonically increasing
                    boolean gapDetected = false;
                    for (LogRecordWithDLSN record : records) {
                        if ((1 != record.getPositionWithinLogSegment()) && (0 != lastPosition) &&
                                (record.getPositionWithinLogSegment() != (lastPosition + 1))) {
                            bkDistributedLogManager.raiseAlert("Gap detected between records at dlsn = {}", record.getDlsn());
                            if (positionGapDetectionEnabled) {
                                setLastException(new DLIllegalStateException("Gap detected between records at dlsn = " + record.getDlsn()));
                                gapDetected = true;
                                break;
                            }
                        }
                        lastPosition = record.getPositionWithinLogSegment();
                    }

                    if (positionGapDetectionEnabled && gapDetected) {
                        continue;
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} : Satisfied promise with {} records", bkLedgerManager.getFullyQualifiedName(), records.size());
                    }
                    PendingReadRequest request = pendingRequests.poll();
                    if (null != request) {
                        request.setValue(records);
                    } else {
                        // We should never get here as we should have exited the loop if
                        // pendingRequests were empty
                        bkDistributedLogManager.raiseAlert("Unexpected condition at dlsn = {}",
                                records.get(0).getDlsn());
                        setLastException(
                            new DLIllegalStateException("Unexpected condition at dlsn = " + records.get(0).getDlsn()));
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
        simulateErrors = true;
        if (null != bkLedgerManager) {
            bkLedgerManager.simulateErrors();
        }
    }

    @VisibleForTesting
    synchronized void disableReadAheadZKNotification() {
        disableReadAheadZKNotification = true;
        bkLedgerManager.disableReadAheadZKNotification();
    }
}

