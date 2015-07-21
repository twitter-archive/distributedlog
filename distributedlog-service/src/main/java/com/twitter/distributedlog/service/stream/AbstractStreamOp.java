package com.twitter.distributedlog.service.stream;

import com.google.common.base.Stopwatch;

import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.ResponseHeader;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

public abstract class AbstractStreamOp<Response> implements StreamOp {
    protected final String stream;
    protected final OpStatsLogger opStatsLogger;
    protected final Promise<Response> result = new Promise<Response>();
    protected final Stopwatch stopwatch = Stopwatch.createUnstarted();

    public AbstractStreamOp(String stream, OpStatsLogger statsLogger) {
        this.stream = stream;
        this.opStatsLogger = statsLogger;
        // start here in case the operation is failed before executing.
        stopwatch.reset().start();
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
    public void preExecute() {
        // Do nothing
    }

    @Override
    public Future<Void> execute(AsyncLogWriter writer) {
        stopwatch.reset().start();
        return executeOp(writer).addEventListener(new FutureEventListener<Response>() {
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
     * @param owner
     *          current owner
     * @param t
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
     * @return future representing the operation.
     */
    protected abstract Future<Response> executeOp(AsyncLogWriter writer);

    // fail the result with the given response header
    protected abstract void fail(ResponseHeader header);

    protected long nextTxId() {
        return System.currentTimeMillis();
    }

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
