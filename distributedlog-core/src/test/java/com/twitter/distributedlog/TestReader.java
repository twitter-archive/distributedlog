package com.twitter.distributedlog;

import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * A Reader wraps reading next logic for testing.
 */
public class TestReader implements FutureEventListener<LogRecordWithDLSN> {

    static final Logger LOG = LoggerFactory.getLogger(TestReader.class);

    final String readerName;
    final DistributedLogManager dlm;
    AsyncLogReader reader;
    final DLSN startDLSN;
    DLSN nextDLSN;
    final boolean simulateErrors;
    int delayMs;
    final ScheduledExecutorService executorService;

    // Latches

    // Latch on notifying reader is ready to read
    final CountDownLatch readyLatch;
    // Latch no notifying reads are completed or errors are encountered
    final CountDownLatch completionLatch;
    // Latch no notifying reads are done.
    final CountDownLatch countLatch;

    // States
    final AtomicBoolean errorsFound;
    final AtomicInteger readCount;
    final AtomicInteger positionReaderCount;

    public TestReader(String name,
                      DistributedLogManager dlm,
                      DLSN startDLSN,
                      boolean simulateErrors,
                      int delayMs,
                      CountDownLatch readyLatch,
                      CountDownLatch countLatch,
                      CountDownLatch completionLatch) {
        this.readerName = name;
        this.dlm = dlm;
        this.startDLSN = startDLSN;
        this.simulateErrors = simulateErrors;
        this.delayMs = delayMs;
        this.readyLatch = readyLatch;
        this.countLatch = countLatch;
        this.completionLatch = completionLatch;
        // States
        this.errorsFound = new AtomicBoolean(false);
        this.readCount = new AtomicInteger(0);
        this.positionReaderCount = new AtomicInteger(0);
        // Executors
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public AtomicInteger getNumReaderPositions() {
        return this.positionReaderCount;
    }

    public AtomicInteger getNumReads() {
        return this.readCount;
    }

    public boolean areErrorsFound() {
        return errorsFound.get();
    }

    private int nextDelayMs() {
        int newDelayMs = Math.min(delayMs * 2, 500);
        if (0 == delayMs) {
            newDelayMs = 10;
        }
        delayMs = newDelayMs;
        return delayMs;
    }

    private void positionReader(final DLSN dlsn) {
        positionReaderCount.incrementAndGet();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncLogReader reader = dlm.getAsyncLogReader(dlsn);
                    if (simulateErrors) {
                        ((BKAsyncLogReaderDLSN) reader).simulateErrors();
                    }
                    nextDLSN = dlsn;
                    LOG.info("Positioned reader {} at {}", readerName, dlsn);
                    TestReader.this.reader = reader;
                    readNext();
                    readyLatch.countDown();
                } catch (IOException exc) {
                    int nextMs = nextDelayMs();
                    LOG.info("Encountered exception {} on opening reader {} at {}, retrying in {} ms",
                            new Object[] { exc, readerName, dlsn, nextMs });
                    positionReader(dlsn);
                }
            }
        };
        executorService.schedule(runnable, delayMs, TimeUnit.MILLISECONDS);
    }

    private void readNext() {
        Future<LogRecordWithDLSN> record = reader.readNext();
        record.addEventListener(this);
    }

    @Override
    public void onSuccess(LogRecordWithDLSN value) {
        try {
            assertTrue(value.getDlsn().compareTo(nextDLSN) >= 0);
            LOG.info("Received record {} from log {} for reader {}",
                    new Object[] { value.getDlsn(), dlm.getStreamName(), readerName });
            assertFalse(value.isControl());
            assertEquals(0, value.getDlsn().getSlotId());
            DLMTestUtil.verifyLargeLogRecord(value);
        } catch (Exception exc) {
            LOG.error("Exception encountered when verifying received log record {} for reader {} :",
                    new Object[] { value.getDlsn(), exc, readerName });
            errorsFound.set(true);
            completionLatch.countDown();
            return;
        }
        readCount.incrementAndGet();
        countLatch.countDown();
        if (countLatch.getCount() <= 0) {
            LOG.info("Reader {} is completed", readerName);
            closeReader();
            completionLatch.countDown();
        } else {
            LOG.info("Reader {} : read count becomes {}, latch = {}",
                    new Object[] { readerName, readCount.get(), countLatch.getCount() });
            nextDLSN = value.getDlsn().getNextDLSN();
            readNext();
        }
    }

    @Override
    public void onFailure(Throwable cause) {
        LOG.error("{} encountered exception on reading next record : ", readerName, cause);
        closeReader();
        nextDelayMs();
        positionReader(nextDLSN);
    }

    private void closeReader() {
        if (null != reader) {
            reader.asyncClose().onFailure(new AbstractFunction1<Throwable, BoxedUnit>() {
                @Override
                public BoxedUnit apply(Throwable cause) {
                    LOG.warn("Exception on closing reader {} : ", readerName, cause);
                    return BoxedUnit.UNIT;
                }
            });
        }
    }

    public void start() {
        positionReader(startDLSN);
    }

    public void stop() {
        closeReader();
        executorService.shutdown();
    }

}
