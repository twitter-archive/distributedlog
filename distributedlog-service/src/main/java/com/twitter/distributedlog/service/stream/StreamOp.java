package com.twitter.distributedlog.service.stream;

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.util.Future;

/**
 * An operation applied to a stream.
 */
public interface StreamOp {
    /**
     * Execute a stream op with the supplied writer.
     *
     * @param writer active writer for applying the change
     * @return a future satisfied when the operation completes execution
     */
    Future<Void> execute(AsyncLogWriter writer);

    /**
     * Invoked before the stream op is executed.
     */
    void preExecute();

    /**
     * Return the response header (containing the status code etc.).
     *
     * @return A future containing the response header or the exception
     *      encountered by the op if it failed.
     */
    Future<ResponseHeader> responseHeader();

    /**
     * Abort the operation with the givem exception.
     */
    void fail(Throwable t);

    /**
     * Return the stream name.
     */
    String streamName();

    /**
     * Stopwatch gives the start time of the operation.
     */
    Stopwatch stopwatch();
}
