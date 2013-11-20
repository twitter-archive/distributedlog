package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.exceptions.RetryableReadException;
import com.twitter.util.Future;
import com.twitter.util.Promise;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BKAsyncLogReaderDLSN implements ZooKeeperClient.ZooKeeperSessionExpireNotifier, AsyncLogReader, Runnable, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogReaderDLSN.class);

    protected final BKDistributedLogManager bkDistributedLogManager;
    protected BKContinuousLogReaderDLSN currentReader = null;
    protected final int readAheadWaitTime;
    private Watcher sessionExpireWatcher = null;
    private boolean endOfStreamEncountered = false;
    private IOException lastException = null;
    private ScheduledExecutorService executorService;
    private ConcurrentLinkedQueue<Promise<LogRecordWithDLSN>> pendingRequests = new ConcurrentLinkedQueue<Promise<LogRecordWithDLSN>>();
    private AtomicLong scheduleCount = new AtomicLong(0);

    public BKAsyncLogReaderDLSN(BKDistributedLogManager bkdlm,
                                ScheduledExecutorService executorService,
                                     String streamIdentifier,
                                     DLSN startDLSN,
                                     int readAheadWaitTime) throws IOException {
        this.bkDistributedLogManager = bkdlm;
        this.readAheadWaitTime = readAheadWaitTime;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
        this.executorService = executorService;
        this.currentReader = new BKContinuousLogReaderDLSN(bkDistributedLogManager, streamIdentifier, startDLSN, true, readAheadWaitTime, true, this);
    }

    @Override
    public void notifySessionExpired() {
        scheduleBackgroundRead();
    }

    private IOException checkClosedOrInError(String operation) {
        if (null != lastException) {
            return lastException;
        }

        try {
            currentReader.checkClosedOrInError(operation);
        } catch (IOException exc) {
            IOException throwExc = exc;
            if (!(exc instanceof EndOfStreamException)) {
                throwExc = new RetryableReadException(currentReader.getFullyQualifiedName(), exc.getMessage(), exc);
            }
            return throwExc;
        }

        return null;
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

        IOException throwExc = checkClosedOrInError("readNext");
        if (null != throwExc) {
            for (Promise<LogRecordWithDLSN> pendingPromise : pendingRequests) {
                pendingPromise.setException(throwExc);
            }
            pendingRequests.clear();
            promise.setException((throwExc));
            return promise;
        }

        boolean queueEmpty = pendingRequests.isEmpty();
        pendingRequests.add(promise);

        if (queueEmpty) {
            scheduleBackgroundRead();
        }

        return promise;
    }

    public void scheduleBackgroundRead() {
        long prevCount = scheduleCount.getAndIncrement();
        if (0 == prevCount) {
            executorService.submit(this);
        }

    }

    @Override
    public void close() throws IOException {
        for (Promise<LogRecordWithDLSN> promise : pendingRequests) {
            promise.setException(new RetryableReadException(currentReader.getFullyQualifiedName(), "Reader is closed"));
            pendingRequests.clear();
        }

        if (null != currentReader) {
            currentReader.close();
        }

        bkDistributedLogManager.unregister(sessionExpireWatcher);
    }


    @Override
    public void run() {
        int iterations = 0;
        LOG.debug("Scheduled Background Reader");
        while(true) {
            LOG.debug("Executing Iteration: {}", iterations++);
            if (pendingRequests.isEmpty()) {
                return;
            }

            IOException throwExc = checkClosedOrInError("readNext");
            if (null != throwExc) {
                for (Promise<LogRecordWithDLSN> promise : pendingRequests) {
                    promise.setException(throwExc);
                }
                pendingRequests.clear();
                return;
            }

            LogRecordWithDLSN record = null;
            try {
                record = currentReader.readNextWithSkip();
            } catch (IOException exc) {
                lastException = exc;
                return;
            }

            if (null != record) {
                LOG.debug("Satisfied promise with record {}", record.getTransactionId());
                Promise<LogRecordWithDLSN> promise = pendingRequests.poll();
                if (null != promise) {
                    promise.setValue(record);
                }
            } else {
                if (0 >= scheduleCount.get()) {
                    return;
                }
                scheduleCount.decrementAndGet();
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
}

