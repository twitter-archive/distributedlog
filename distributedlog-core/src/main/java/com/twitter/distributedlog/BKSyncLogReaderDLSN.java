package com.twitter.distributedlog;

import com.twitter.distributedlog.callback.ReadAheadCallback;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.util.FutureEventListener;

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
    private boolean closed = false;

    BKSyncLogReaderDLSN(DistributedLogConfiguration conf,
                        BKAsyncLogReaderDLSN reader,
                        ScheduledExecutorService executorService) {
        this.maxNumCachedRecords = conf.getReadAheadMaxEntries();
        this.maxReadAheadWaitTime = conf.getReadAheadWaitTime();
        this.reader = reader;
        this.executorService = executorService;
        this.readAheadRecords = new LinkedBlockingQueue<LogRecordWithDLSN>();
        scheduleReadNext();
    }

    private void scheduleReadNext() {
        if (closed) {
            return;
        }
        this.executorService.submit(this);
    }

    private synchronized void invokeReadAheadCallback() {
        if (null != readAheadCallback) {
            readAheadCallback.resumeReadAhead();
            readAheadCallback = null;
        }
    }

    private synchronized void setReadAheadCallback(ReadAheadCallback callback) {
        this.readAheadCallback = callback;
        if (readAheadRecords.size() < maxNumCachedRecords) {
            invokeReadAheadCallback();
        }
    }

    // Background Read Future Listener

    @Override
    public void resumeReadAhead() {
        scheduleReadNext();
    }

    @Override
    public void onSuccess(LogRecordWithDLSN record) {
        readAheadRecords.add(record);
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
                if (reader.bkLedgerManager.isReadAheadCaughtUp()) {
                    record = readAheadRecords.poll(maxReadAheadWaitTime,
                            TimeUnit.MILLISECONDS);
                } else {
                    // reader is still catching up, waiting for next record
                    while (!reader.bkLedgerManager.isReadAheadCaughtUp()
                            && record == null) {
                        record = readAheadRecords.poll(maxReadAheadWaitTime,
                                TimeUnit.MILLISECONDS);
                    }
                }
            } catch (InterruptedException e) {
                throw new DLInterruptedException("Interrupted on waiting next available log record for stream "
                        + reader.getStreamName(), e);
            }
        }
        if (null != record) {
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
    public void close() throws IOException {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        reader.close();
    }
}
