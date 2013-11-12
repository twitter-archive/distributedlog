package com.twitter.distributedlog;

import com.twitter.util.Future;
import com.twitter.util.Promise;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.Watcher;


public class BKAsyncLogReaderDLSN implements ZooKeeperClient.ZooKeeperSessionExpireNotifier, AsyncLogReader, Runnable {
    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogPartitionReadHandler bkLedgerManager;
    protected BKContinuousLogReaderDLSN currentReader = null;
    protected final int readAheadWaitTime;
    private Watcher sessionExpireWatcher = null;
    private boolean zkSessionExpired = false;
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
        this.bkLedgerManager = bkDistributedLogManager.createReadLedgerHandler(streamIdentifier);
        this.readAheadWaitTime = readAheadWaitTime;
        sessionExpireWatcher = bkDistributedLogManager.registerExpirationHandler(this);
        this.executorService = executorService;
        this.currentReader = new BKContinuousLogReaderDLSN(bkDistributedLogManager, streamIdentifier, startDLSN, true, readAheadWaitTime, true);
    }

    @Override
    public void notifySessionExpired() {
        zkSessionExpired = true;
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                for (Promise<LogRecordWithDLSN> promise : pendingRequests) {
                    promise.setException(new AlreadyClosedException("ZooKeeper Session expired"));
                }
            }
        });
    }

    private void checkClosedOrInError(String operation) throws IOException {
        if (zkSessionExpired) {
            LOG.error("Executing " + operation + " after losing connection to zookeeper");
            throw new AlreadyClosedException("Executing " + operation + " after losing connection to zookeeper");
        }

        currentReader.checkClosedOrInError(operation);

    }

    /**
     * @param timeout - timeout value
     * @param timeUnit - units associated with the timeout value
     * @return A promise that when satisfied will contain the Log Record with its DLSN;
     *         The Future may timeout if there is no record to return within the specified timeout
     */
    @Override
    public synchronized Future<LogRecordWithDLSN> readNext(long timeout, TimeUnit timeUnit) throws IOException {
        checkClosedOrInError("readNext");

        boolean queueEmpty = pendingRequests.isEmpty();
        Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
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
            promise.setException(new AlreadyClosedException("ZooKeeper Session expired"));
        }

        if (null != currentReader) {
            currentReader.close();
        }

        if (null != bkLedgerManager) {
            bkLedgerManager.close();
        }

        if (null != lastException) {
            throw lastException;
        }

        bkDistributedLogManager.unregister(sessionExpireWatcher);
    }


    @Override
    public void run() {
        while(true) {
            if (pendingRequests.isEmpty()) {
                return;
            }

            try {
                LogRecordWithDLSN record = currentReader.readNext(false);
            } catch (IOException exc) {
                lastException = exc;
                return;
            }

            if (null != record) {
                Promise<LogRecordWithDLSN> promise = pendingRequests.poll();
                if (null != promise) {
                    promise.setValue(record);
                } else {
                    // This can only happen if there was a ZK session expire
                    // after the loop started
                    assert(zkSessionExpired);
                }
            } else {
                if (0 == scheduleCount.decrementAndGet()) {
                    return;
                }
            }
        }

    }
}

