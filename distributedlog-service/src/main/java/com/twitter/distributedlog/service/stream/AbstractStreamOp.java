package com.twitter.distributedlog.service.stream;

import com.google.common.base.Stopwatch;

import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.ProtocolUtils;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.ResponseHeader;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStreamOp<Response> implements StreamOp {
    static final Logger logger = LoggerFactory.getLogger(AbstractStreamOp.class);
    protected final String stream;
    protected final OpStatsLogger opStatsLogger;
    protected final Promise<Response> result = new Promise<Response>();
    protected final Stopwatch stopwatch = Stopwatch.createUnstarted();
    protected final Long checksum;

    // CRC32 uses off-heap mem and can cause memory issues if allocations are not
    // limited (ex. per request). Use ThreadLocal to ensure we have just one per
    // thread.
    protected final ThreadLocal<CRC32> requestCRC;

    public AbstractStreamOp(String stream,
                            OpStatsLogger statsLogger,
                            Long checksum,
                            ThreadLocal<CRC32> requestCRC) {
        this.stream = stream;
        this.opStatsLogger = statsLogger;
        // start here in case the operation is failed before executing.
        stopwatch.reset().start();
        this.checksum = checksum;
        this.requestCRC = requestCRC;
    }

    @Override
    public String streamName() {
        return stream;
    }

    @Override
    public Stopwatch stopwatch() {
        return stopwatch;
    }

    @Override
    public void preExecute() throws DLException {
        if (null != checksum) {
            Long serverChecksum = computeChecksum();
            if (null != serverChecksum && !checksum.equals(serverChecksum)) {
                throw new UnexpectedException("Thrift call arguments failed checksum");
            }
        }
    }

    @Override
    public Long computeChecksum() {
        return null;
    }

    @Override
    public Future<Void> execute(AsyncLogWriter writer, Sequencer sequencer, Object txnLock) {
        stopwatch.reset().start();
        return executeOp(writer, sequencer, txnLock)
                .addEventListener(new FutureEventListener<Response>() {
            @Override
            public void onSuccess(Response response) {
                opStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                result.setValue(response);
            }
            @Override
            public void onFailure(Throwable cause) {
            }
        }).voided();
    }

    /**
     * Fail with current <i>owner</i> and its reason <i>t</i>
     *
     * @param cause
     *          failure reason
     */
    @Override
    public void fail(Throwable cause) {
        if (cause instanceof OwnershipAcquireFailedException) {
            // Ownership exception is a control exception, not an error, so we don't stat
            // it with the other errors.
            OwnershipAcquireFailedException oafe = (OwnershipAcquireFailedException) cause;
            fail(ResponseUtils.ownerToHeader(oafe.getCurrentOwner()));
        } else {
            opStatsLogger.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            fail(ResponseUtils.exceptionToHeader(cause));
        }
    }

    /**
     * Return the full response, header and body.
     *
     * @return A future containing the response or the exception
     *      encountered by the op if it failed.
     */
    public Future<Response> result() {
        return result;
    }

    /**
     * Execute the operation and return its corresponding response.
     *
     * @param writer
     *          writer to execute the operation.
     * @param sequencer
     *          sequencer used for generating transaction id for stream operations
     * @param txnLock
     *          transaction lock to guarantee ordering of transaction id
     * @return future representing the operation.
     */
    protected abstract Future<Response> executeOp(AsyncLogWriter writer,
                                                  Sequencer sequencer,
                                                  Object txnLock);

    // fail the result with the given response header
    protected abstract void fail(ResponseHeader header);

    protected static OpStatsLogger requestStat(StatsLogger statsLogger, String opName) {
        return requestLogger(statsLogger).getOpStatsLogger(opName);
    }

    protected static StatsLogger requestLogger(StatsLogger statsLogger) {
        return statsLogger.scope("request");
    }

    protected static StatsLogger requestScope(StatsLogger statsLogger, String scope) {
        return requestLogger(statsLogger).scope(scope);
    }
}
