package com.twitter.distributedlog;

import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

class BKTransmitPacket {

    private boolean isControl;
    private long logSegmentSequenceNo;
    private DLSN lastDLSN;
    private List<Promise<DLSN>> promiseList;
    private Promise<Integer> transmitComplete;
    private long transmitTime;
    private long maxTxId;
    private LogRecordSet.Writer writer;

    BKTransmitPacket(String logName,
                     long logSegmentSequenceNo,
                     int initialBufferSize,
                     boolean envelopeBeforeTransmit,
                     CompressionCodec.Type codec,
                     StatsLogger statsLogger) {
        this.logSegmentSequenceNo = logSegmentSequenceNo;
        this.promiseList = new LinkedList<Promise<DLSN>>();
        this.isControl = false;
        this.transmitComplete = new Promise<Integer>();
        this.writer = LogRecordSet.newLogRecordSet(
                logName,
                initialBufferSize * 6 / 5,
                envelopeBeforeTransmit,
                codec,
                statsLogger);
        this.maxTxId = Long.MIN_VALUE;
    }

    public void reset() {
        if (null != promiseList) {
            // Likely will have to move promise fulfillment to a separate thread
            // so safest to just create a new list so the old list can move with
            // with the thread, hence avoiding using clear to measure accurate GC
            // behavior
            cancelPromises(BKException.Code.InterruptedException);
        }
        promiseList = new LinkedList<Promise<DLSN>>();
        writer.reset();
    }

    public long getLogSegmentSequenceNo() {
        return logSegmentSequenceNo;
    }

    public void addToPromiseList(Promise<DLSN> nextPromise, long txId) {
        promiseList.add(nextPromise);
        maxTxId = Math.max(maxTxId, txId);
    }

    public LogRecordSet.Writer getRecordSetWriter() {
        return writer;
    }

    private void satisfyPromises(long entryId) {
        long nextSlotId = 0;
        for(Promise<DLSN> promise : promiseList) {
            promise.setValue(new DLSN(logSegmentSequenceNo, entryId, nextSlotId));
            nextSlotId++;
        }
        promiseList.clear();
    }

    private void cancelPromises(int transmitResult) {
        cancelPromises(FutureUtils.transmitException(transmitResult));
    }

    public void cancelPromises(Throwable t) {
        for(Promise<DLSN> promise : promiseList) {
            promise.setException(t);
        }
        promiseList.clear();
    }

    public void processTransmitComplete(long entryId, int transmitResult) {
        if (transmitResult != BKException.Code.OK) {
            cancelPromises(transmitResult);
        } else {
            satisfyPromises(entryId);
        }
    }

    public DLSN finalize(long entryId, int transmitResult) {
        if (transmitResult == BKException.Code.OK) {
            lastDLSN = new DLSN(logSegmentSequenceNo, entryId, promiseList.size() - 1);
        }
        return lastDLSN;
    }

    public void setControl(boolean control) {
        isControl = control;
    }

    public boolean isControl() {
        return isControl;
    }

    public int awaitTransmitComplete(long timeout, TimeUnit unit) throws Exception {
        return Await.result(transmitComplete, Duration.fromTimeUnit(timeout, unit));
    }

    public Future<Integer> awaitTransmitComplete() {
        return transmitComplete;
    }

    public void setTransmitComplete(int transmitResult) {
        transmitComplete.setValue(transmitResult);
    }

    public void notifyTransmit() {
        transmitTime = System.nanoTime();
    }

    public long getTransmitTime() {
        return transmitTime;
    }

    public long getWriteCount() {
        return promiseList.size();
    }

    public long getMaxTxId() {
        return maxTxId;
    }

}
