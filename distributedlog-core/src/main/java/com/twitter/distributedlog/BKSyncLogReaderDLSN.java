package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.twitter.distributedlog.callback.ReadAheadCallback;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.EndOfStreamException;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Synchronous Log Reader based on {@link AsyncLogReader}
 */
class BKSyncLogReaderDLSN implements LogReader, Runnable, FutureEventListener<LogRecordWithDLSN>, ReadAheadCallback {

    private final BKAsyncLogReaderDLSN reader;
    private final ScheduledExecutorService executorService;
    private final LinkedBlockingQueue<LogRecordWithDLSN> readAheadRecords;
    private final AtomicReference<IOException> readerException =
            new AtomicReference<IOException>(null);
    private final int maxNumCachedRecords;
    private final int maxReadAheadWaitTime;
    private ReadAheadCallback readAheadCallback = null;
    private Promise<Void> closeFuture;
    private final Optional<Long> startTransactionId;
    private final DLSN startDLSN;
    private DLSN lastSeenDLSN = DLSN.InvalidDLSN;
    // lock on variables that would be accessed by both background threads and foreground threads
    private final Object sharedLock = new Object();

    BKSyncLogReaderDLSN(DistributedLogConfiguration conf,
                        BKAsyncLogReaderDLSN reader,
                        ScheduledExecutorService executorService,
                        Optional<Long> startTransactionId) {
        this.maxNumCachedRecords = conf.getReadAheadMaxRecords();
        this.maxReadAheadWaitTime = conf.getReadAheadWaitTime();
        this.reader = reader;
        this.executorService = executorService;
        this.readAheadRecords = new LinkedBlockingQueue<LogRecordWithDLSN>();
        this.startTransactionId = startTransactionId;
        this.startDLSN = reader.getStartDLSN();
        scheduleReadNext();
    }

    @VisibleForTesting
    BKAsyncLogReaderDLSN getAsyncReader() {
        return reader;
    }

    private void scheduleReadNext() {
        synchronized (sharedLock) {
            if (null != closeFuture) {
                return;
            }
        }
        this.executorService.submit(this);
    }

    private void invokeReadAheadCallback() {
        synchronized (sharedLock) {
            if (null != readAheadCallback) {
                readAheadCallback.resumeReadAhead();
                readAheadCallback = null;
            }
        }
    }

    private void setReadAheadCallback(ReadAheadCallback callback) {
        synchronized (sharedLock) {
            this.readAheadCallback = callback;
            if (readAheadRecords.size() < maxNumCachedRecords) {
                invokeReadAheadCallback();
            }
        }
    }

    private void setLastSeenDLSN(DLSN dlsn) {
        synchronized (sharedLock) {
            this.lastSeenDLSN = dlsn;
        }
    }

    // Background Read Future Listener

    @Override
    public void resumeReadAhead() {
        scheduleReadNext();
    }

    @Override
    public void onSuccess(LogRecordWithDLSN record) {
        setLastSeenDLSN(record.getDlsn());
        if (!startTransactionId.isPresent() || record.getTransactionId() >= startTransactionId.get()) {
            readAheadRecords.add(record);
        }
        if (readAheadRecords.size() >= maxNumCachedRecords) {
            setReadAheadCallback(this);
        } else {
            scheduleReadNext();
        }
    }

    @Override
    public void onFailure(Throwable cause) {
        if (cause instanceof IOException) {
            readerException.compareAndSet(null, (IOException) cause);
        } else {
            readerException.compareAndSet(null, new IOException("Encountered exception on reading "
                    + reader.getStreamName() + " : ", cause));
        }
    }

    // Background Read

    @Override
    public void run() {
        this.reader.readNext().addEventListener(this);
    }

    @Override
    public synchronized LogRecordWithDLSN readNext(boolean nonBlocking)
            throws IOException {
        if (null != readerException.get()) {
            throw readerException.get();
        }
        LogRecordWithDLSN record = null;
        if (nonBlocking) {
            record = readAheadRecords.poll();
        } else {
            try {
                // reader is still catching up, waiting for next record
                while (!reader.bkLedgerManager.isReadAheadCaughtUp()
                        && null == readerException.get()
                        && null == record) {
                    record = readAheadRecords.poll(maxReadAheadWaitTime,
                            TimeUnit.MILLISECONDS);
                }
                // reader caught up
                boolean shallWait = true;
                while (shallWait
                        && reader.bkLedgerManager.isReadAheadCaughtUp()
                        && null == record
                        && null == readerException.get()) {
                    record = readAheadRecords.poll(maxReadAheadWaitTime,
                            TimeUnit.MILLISECONDS);
                    if (null != record) {
                        break;
                    }
                    synchronized (sharedLock) {
                        DLSN lastDLSNSeenByReadAhead =
                                reader.bkLedgerManager.readAheadCache.getLastReadAheadUserDLSN();

                        // if last seen DLSN by reader is same as the one seen by ReadAhead
                        // that means that reader is caught up with ReadAhead and ReadAhead
                        // is caught up with stream
                        shallWait = DLSN.InitialDLSN != lastDLSNSeenByReadAhead
                                && lastSeenDLSN.compareTo(lastDLSNSeenByReadAhead) < 0
                                && startDLSN.compareTo(lastDLSNSeenByReadAhead) <= 0;
                    }
                }
            } catch (InterruptedException e) {
                throw new DLInterruptedException("Interrupted on waiting next available log record for stream "
                        + reader.getStreamName(), e);
            }
        }
        if (null != readerException.get()) {
            throw readerException.get();
        }
        if (null != record) {
            if (record.isEndOfStream()) {
                EndOfStreamException eos = new EndOfStreamException("End of Stream Reached for "
                                        + reader.bkLedgerManager.getFullyQualifiedName());
                readerException.compareAndSet(null, eos);
                throw eos;
            }
            invokeReadAheadCallback();
        }
        return record;
    }

    @Override
    public synchronized List<LogRecordWithDLSN> readBulk(boolean nonBlocking, int numLogRecords)
            throws IOException {
        LinkedList<LogRecordWithDLSN> retList =
                new LinkedList<LogRecordWithDLSN>();

        int numRead = 0;
        LogRecordWithDLSN record = readNext(nonBlocking);
        while ((null != record)) {
            retList.add(record);
            numRead++;
            if (numRead >= numLogRecords) {
                break;
            }
            record = readNext(nonBlocking);
        }
        return retList;
    }

    @Override
    public Future<Void> asyncClose() {
        Promise<Void> closePromise;
        synchronized (sharedLock) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closeFuture = closePromise = new Promise<Void>();
        }
        reader.asyncClose().proxyTo(closePromise);
        return closePromise;
    }

    @Override
    public void close() throws IOException {
        FutureUtils.result(asyncClose());
    }

    //
    // Test Methods
    //
    @VisibleForTesting
    void disableReadAheadZKNotification() {
        reader.bkLedgerManager.disableReadAheadZKNotification();
    }

    @VisibleForTesting
    LedgerReadPosition getReadAheadPosition() {
        if (null != reader.bkLedgerManager.readAheadWorker) {
            return reader.bkLedgerManager.readAheadWorker.getNextReadAheadPosition();
        }
        return null;
    }
}
