package com.twitter.distributedlog;

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

import java.util.concurrent.TimeUnit;

class BKTransmitPacket {

    private final LogRecordSetBuffer recordSet;
    private final long transmitTime;
    private final Promise<Integer> transmitComplete;

    BKTransmitPacket(LogRecordSetBuffer recordSet) {
        this.recordSet = recordSet;
        this.transmitTime = System.nanoTime();
        this.transmitComplete = new Promise<Integer>();
    }

    LogRecordSetBuffer getRecordSet() {
        return recordSet;
    }

    Promise<Integer> getTransmitFuture() {
        return transmitComplete;
    }

    /**
     * Complete the transmit with result code <code>transmitRc</code>.
     * <p>It would notify all the waiters that are waiting via {@link #awaitTransmitComplete(long, TimeUnit)}
     * or {@link #addTransmitCompleteListener(FutureEventListener)}.
     *
     * @param transmitResult
     *          transmit result code.
     */
    public void notifyTransmitComplete(int transmitResult) {
        transmitComplete.setValue(transmitResult);
    }

    /**
     * Register a transmit complete listener.
     * <p>The listener will be triggered with transmit result when transmit completes.
     * The method should be non-blocking.
     *
     * @param transmitCompleteListener
     *          listener on transmit completion
     * @see #awaitTransmitComplete(long, TimeUnit)
     */
    void addTransmitCompleteListener(FutureEventListener<Integer> transmitCompleteListener) {
        transmitComplete.addEventListener(transmitCompleteListener);
    }

    /**
     * Await for the transmit to be complete
     *
     * @param timeout
     *          wait timeout
     * @param unit
     *          wait timeout unit
     */
    int awaitTransmitComplete(long timeout, TimeUnit unit)
        throws Exception {
        return Await.result(transmitComplete,
                Duration.fromTimeUnit(timeout, unit));
    }

    public long getTransmitTime() {
        return transmitTime;
    }

}
