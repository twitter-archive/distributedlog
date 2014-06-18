package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.RetryableReadException;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Throw;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;


class BKAsyncLogReaderDLSN implements ZooKeeperClient.ZooKeeperSessionExpireNotifier, AsyncLogReader, Runnable, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogReaderDLSN.class);

    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogPartitionReadHandler bkLedgerManager;
    private Watcher sessionExpireWatcher = null;
    private boolean endOfStreamEncountered = false;
    private AtomicReference<Throwable> lastException = new AtomicReference<Throwable>();
    private ScheduledExecutorService executorService;
    private ConcurrentLinkedQueue<Promise<LogRecordWithDLSN>> pendingRequests = new ConcurrentLinkedQueue<Promise<LogRecordWithDLSN>>();
    private AtomicLong scheduleCount = new AtomicLong(0);
    private boolean simulateErrors = false;
    private static OpStatsLogger futureSatisfyLatency = null;
    private static OpStatsLogger scheduleLatency = null;
    private static OpStatsLogger backgroundReaderRunTime = null;
    private Stopwatch scheduleDelayStopwatch;
    private final DLSN startDLSN;
    private boolean readAheadStarted = false;
    private int lastCount = 0;

    public BKAsyncLogReaderDLSN(BKDistributedLogManager bkdlm,
                                ScheduledExecutorService executorService,
                                     String streamIdentifier,
                                     DLSN startDLSN,
                                     StatsLogger statsLogger) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
        this.executorService = executorService;
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier, this);
        LOG.debug("Starting async reader at {}", startDLSN);
        this.startDLSN = startDLSN;
        StatsLogger asyncReaderStatsLogger = statsLogger.scope("async_reader");

        if (null == futureSatisfyLatency) {
            futureSatisfyLatency = asyncReaderStatsLogger.getOpStatsLogger("future_set");
        }

        if (null == scheduleLatency) {
            scheduleLatency = asyncReaderStatsLogger.getOpStatsLogger("schedule");
        }

        if (null == backgroundReaderRunTime) {
            backgroundReaderRunTime = asyncReaderStatsLogger.getOpStatsLogger("background_read");
        }
    }

    @Override
    public void notifySessionExpired() {
        // ZK Session notification is an indication to check if this has resulted in a fatal error
        // of the underlying reader, in itself this reader doesnt error out unless the underlying
        // reader has hit an error
        scheduleBackgroundRead();
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

        if (null != lastException.get()) {
            LOG.trace("Cancelling pending reads");
            cancelAllPendingReads(lastException.get());
            return true;
        }

        return false;
    }

    private void setLastException(Throwable exc) {
        lastException.set(exc);
        if (!(exc instanceof EndOfStreamException)) {
            lastException.set(new RetryableReadException(bkLedgerManager.getFullyQualifiedName(), exc.getMessage(), exc));
        }
    }

    /**
     * @param timeout - timeout value
     * @param timeUnit - units associated with the timeout value
     * @return A promise that when satisfied will contain the Log Record with its DLSN;
     *         The Future may timeout if there is no record to return within the specified timeout
     */
    @Override
    public synchronized Future<LogRecordWithDLSN> readNext() {
        Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();

        if (!readAheadStarted) {
            boolean exists = false;
            try {
                exists = bkLedgerManager.doesLogExist();
            } catch (IOException ioe) {
                setLastException(ioe);
            }

            if (!exists) {
                setLastException(new LogNotFoundException(String.format("Log %s does not exist or has been deleted", bkLedgerManager.getFullyQualifiedName())));
            } else {
                bkLedgerManager.startReadAhead(new LedgerReadPosition(startDLSN), simulateErrors);
                readAheadStarted = true;
            }
        }

        if (checkClosedOrInError("readNext")) {
            Stopwatch stopwatch = new Stopwatch().start();
            promise.setException(lastException.get());
            futureSatisfyLatency.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            return promise;
        }

        boolean queueEmpty = pendingRequests.isEmpty();
        pendingRequests.add(promise);

        if (queueEmpty) {
            scheduleBackgroundRead();
        }

        return promise;
    }

    public synchronized void scheduleBackgroundRead() {
        long prevCount = scheduleCount.getAndIncrement();
        if (0 == prevCount) {
            scheduleDelayStopwatch = new Stopwatch().start();
            executorService.submit(this);
        }

    }

    @Override
    public void close() throws IOException {
        cancelAllPendingReads(new RetryableReadException(
            bkLedgerManager.getFullyQualifiedName(), "Reader was closed"));

        bkLedgerManager.close();

        bkDistributedLogManager.unregister(sessionExpireWatcher);
    }

    private void cancelAllPendingReads(Throwable throwExc) {
        for (Promise<LogRecordWithDLSN> promise : pendingRequests) {
            Stopwatch stopwatch = new Stopwatch().start();
            promise.updateIfEmpty(new Throw(throwExc));
            futureSatisfyLatency.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
        }
        pendingRequests.clear();
    }


    @Override
    public void run() {
        synchronized(scheduleCount) {
            if (null != scheduleDelayStopwatch) {
                scheduleLatency.registerSuccessfulEvent(scheduleDelayStopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                scheduleDelayStopwatch = null;
            }

            Stopwatch runTime = new Stopwatch().start();
            int iterations = 0;
            long scheduleCountLocal = scheduleCount.get();
            LOG.debug("{}: Scheduled Background Reader", bkLedgerManager.getFullyQualifiedName());
            while(true) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{}: Executing Iteration: {}", bkLedgerManager.getFullyQualifiedName(), iterations++);
                }

                Promise<LogRecordWithDLSN> nextPromise = null;
                synchronized(this) {
                    nextPromise = pendingRequests.peek();

                    // Queue is empty, nothing to read, return
                    if (null == nextPromise) {
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
                    if (nextPromise.isInterrupted().isDefined()) {
                        setLastException(nextPromise.isInterrupted().get());
                    }
                }

                if (checkClosedOrInError("readNext")) {
                    if (!(lastException.get().getCause() instanceof LogNotFoundException)) {
                        LOG.warn("{}: Exception", bkLedgerManager.getFullyQualifiedName(), lastException.get());
                    }
                    backgroundReaderRunTime.registerFailedEvent(runTime.stop().elapsed(TimeUnit.MICROSECONDS));
                    return;
                }

                LogRecordWithDLSN record = null;
                try {
                    // Fail 10% of the requests when asked to simulate errors
                    if (simulateErrors && Utils.randomPercent(10)) {
                        throw new IOException("Reader Simulated Exception");
                    }
                    do {
                        record = bkLedgerManager.getNextReadAheadRecord();
                    } while (null != record && (record.isControl() || (record.getDlsn().compareTo(startDLSN) < 0)));
                } catch (IOException exc) {
                    setLastException(exc);
                    if (!(exc instanceof LogNotFoundException)) {
                        LOG.warn("{} : read with skip Exception", bkLedgerManager.getFullyQualifiedName(), lastException.get());
                    }
                    continue;
                }

                if (null != record) {
                    // Verify that the count is contiguous and monotonically increasing
                    //
                    if ((1 != record.getCount()) && (0 != lastCount) &&
                        (record.getCount() != (lastCount + 1))) {
                        bkDistributedLogManager.raiseAlert("Gap detected between records at dlsn = {}", record.getDlsn());
                        setLastException(new DLIllegalStateException("Gap detected between records at dlsn = " + record.getDlsn()));
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("{} : Satisfied promise with record {}", bkLedgerManager.getFullyQualifiedName(), record.getTransactionId());
                        }
                        Promise<LogRecordWithDLSN> promise = pendingRequests.poll();
                        if (null != promise) {
                            Stopwatch stopwatch = new Stopwatch().start();
                            lastCount = record.getCount();
                            promise.setValue(record);
                            futureSatisfyLatency.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                        }
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
}

