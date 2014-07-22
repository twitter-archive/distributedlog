package com.twitter.distributedlog.service;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.BKUnPartitionedAsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.util.Future;
import com.twitter.util.Future$;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Try;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Charsets.UTF_8;

class DistributedLogServiceImpl implements DistributedLogService.ServiceIface {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServiceImpl.class);

    static enum StreamStatus {
        UNINITIALIZED(-1),
        INITIALIZING(0),
        INITIALIZED(1),
        // if a stream is in failed state, it could be retried immediately.
        // a stream will be put in failed state when encountered any stream exception.
        FAILED(-2),
        // if a stream is in backoff state, it would backoff for a while.
        // a stream will be put in backoff state when failed to acquire the ownership.
        BACKOFF(-3),
        CLOSING(-4),
        CLOSED(-5);

        final int code;

        StreamStatus(int code) {
            this.code = code;
        }

        int getCode() {
            return code;
        }
    }

    static enum ServerMode {
        DURABLE,
        MEM
    }

    static ResponseHeader ownerToResponseHeader(String owner) {
        return new ResponseHeader(StatusCode.FOUND).setLocation(owner);
    }

    static ResponseHeader successResponseHeader() {
        return new ResponseHeader(StatusCode.SUCCESS);
    }

    static ResponseHeader responseHeader(StatusCode code, String errMsg) {
        return new ResponseHeader(code);
    }

    static ResponseHeader exceptionToResponseHeader(Throwable t) {
        ResponseHeader response = new ResponseHeader();
        if (t instanceof DLException) {
            DLException dle = (DLException) t;
            if (dle instanceof OwnershipAcquireFailedException) {
                response.setLocation(((OwnershipAcquireFailedException) dle).getCurrentOwner());
            }
            response.setCode(dle.getCode());
            response.setErrMsg(dle.getMessage());
        } else {
            response.setCode(StatusCode.INTERNAL_SERVER_ERROR);
            response.setErrMsg("Internal server error : " + t.getMessage());
        }
        return response;
    }

    static ConcurrentMap<String, String> ownerToHosts = new ConcurrentHashMap<String, String>();

    static String getHostByOwner(String owner) {
        String host = ownerToHosts.get(owner);
        if (null == host) {
            try {
                host = DLSocketAddress.deserialize(owner).getSocketAddress().toString();
            } catch (IOException e) {
                host = owner;
            }
            String oldHost = ownerToHosts.putIfAbsent(owner, host);
            if (null != oldHost) {
                host = oldHost;
            }
        }
        return host;
    }

    // WriteResponse
    WriteResponse writeResponse(ResponseHeader responseHeader) {
        countStatusCode(responseHeader.getCode());
        return new WriteResponse(responseHeader);
    }

    abstract class StreamOp {
        final String stream;
        final Stopwatch stopwatch = new Stopwatch();
        final WriteContext context;
        final OpStatsLogger opStatsLogger;

        StreamOp(String stream, WriteContext context, OpStatsLogger statsLogger) {
            this.stream = stream;
            this.context = context;
            this.opStatsLogger = statsLogger;
            // start here in case the operation is failed before executing.
            stopwatch.reset().start();
        }

        long nextTxId() {
            return System.currentTimeMillis();
        }

        boolean hasTried(String owner) {
            return context.isSetTriedHosts() && context.getTriedHosts().contains(getHostByOwner(owner));
        }

        Future<Void> execute(AsyncLogWriter writer) {
            stopwatch.reset().start();
            return executeOp(writer);
        }

        /**
         * Execute the operation and return its corresponding response.
         *
         * @param writer
         *          writer to execute the operation.
         * @return future representing the operation.
         */
        abstract Future<Void> executeOp(AsyncLogWriter writer);

        /**
         * Fail with current <i>owner</i> and its reason <i>t</i>
         *
         * @param owner
         *          current owner
         * @param t
         *          failure reason
         */
        void fail(String owner, Throwable t) {
            opStatsLogger.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            if (null != owner) {
                redirects.inc();
                fail(ownerToResponseHeader(owner));
            } else {
                fail(exceptionToResponseHeader(t));
            }
        }

        void fail(StatusCode code, String message) {
            fail(responseHeader(code, message));
        }
         
        // fail the result with the given response header
        abstract void fail(ResponseHeader header);
    }

    class StreamOpCompletionListener<T> implements FutureEventListener<T> {
        
        OpStatsLogger opStatsLogger;
        Promise<T> result;
        Stopwatch stopwatch;

        public StreamOpCompletionListener(OpStatsLogger opStatsLogger, Promise<T> result, Stopwatch stopwatch) {
            this.opStatsLogger = opStatsLogger;
            this.result = result;
            this.stopwatch = stopwatch;
        }

        @Override
        public void onSuccess(T response) {
            opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            result.setValue(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            // nop, as failure will be handled in {@link Stream.doExecuteOp}.
        }
    }

    class BulkWriteOp extends StreamOp {

        final List<ByteBuffer> buffers;
        final Promise<BulkWriteResponse> result = new Promise<BulkWriteResponse>();
        
        BulkWriteOp(String stream, List<ByteBuffer> buffers, WriteContext context) {
            super(stream, context, bulkWriteStat);
            this.buffers = buffers;
        }

        @Override
        public Future<Void> executeOp(AsyncLogWriter writer) {
            
            // Need to convert input buffers to LogRecords.
            List<LogRecord> records = asRecordList(buffers);
            Future<List<Future<DLSN>>> futureList = writer.writeBulk(records);
            
            // Collect into a list of tries to make it easier to extract exception or DLSN.
            Future<List<Try<DLSN>>> writes = asTryList(futureList);

            Future<BulkWriteResponse> response = writes.map(
                new AbstractFunction1<List<Try<DLSN>>, BulkWriteResponse>() {
                    @Override
                    public BulkWriteResponse apply(List<Try<DLSN>> results) {

                        // Considered a success at batch level even if no individual writes succeeed.
                        // The reason is that its impossible to make an appropriate decision re retries without
                        // individual buffer failure reasons.
                        List<WriteResponse> writeResponses = new ArrayList<WriteResponse>(results.size());
                        BulkWriteResponse bulkWriteResponse = 
                            new BulkWriteResponse(successResponseHeader()).setWriteResponses(writeResponses);

                        // Translate all futures to write responses.
                        Iterator<Try<DLSN>> iterator = results.iterator();
                        while (iterator.hasNext()) {
                            Try<DLSN> completedFuture = iterator.next();
                            try {
                                DLSN dlsn = completedFuture.get();
                                WriteResponse writeResponse = new WriteResponse(successResponseHeader()).setDlsn(dlsn.serialize());
                                writeResponses.add(writeResponse);
                            } catch (Throwable t) {
                                WriteResponse writeResponse = new WriteResponse(exceptionToResponseHeader(t));
                                writeResponses.add(writeResponse);
                            }
                        }
                        
                        return bulkWriteResponse;
                    }
                }
            );

            // FlatMap to void preserves exception without passing on result
            StreamOpCompletionListener<BulkWriteResponse> completionListener = 
                new StreamOpCompletionListener<BulkWriteResponse>(opStatsLogger, result, stopwatch); 
            return response.addEventListener(completionListener).voided();
        }

        List<LogRecord> asRecordList(List<ByteBuffer> buffers) {
            List<LogRecord> records = new ArrayList(buffers.size()); 
            for (ByteBuffer buffer : buffers) {
                byte[] payload = new byte[buffer.remaining()];
                buffer.get(payload);
                records.add(new LogRecord(nextTxId(), payload));
            }
            return records;
        }

        Future<List<Try<DLSN>>> asTryList(Future<List<Future<DLSN>>> futureList) {
            return futureList.flatMap(new AbstractFunction1<List<Future<DLSN>>, Future<List<Try<DLSN>>>>() {
                @Override
                public Future<List<Try<DLSN>>> apply(List<Future<DLSN>> results) {
                    return Future$.MODULE$.collectToTry(results);
                }
            });
        }

        @Override 
        void fail(ResponseHeader header) {
            result.setValue(new BulkWriteResponse(header));
        }
    }

    abstract class AbstractWriteOp extends StreamOp {

        final Promise<WriteResponse> result = new Promise<WriteResponse>();
        
        AbstractWriteOp(String stream, WriteContext context, OpStatsLogger statsLogger) {
            super(stream, context, statsLogger);
        }

        @Override
        public Future<Void> executeOp(AsyncLogWriter writer) {
            // flatMap to void, preserves exception, discards result
            StreamOpCompletionListener<WriteResponse> completionListener = 
                new StreamOpCompletionListener<WriteResponse>(opStatsLogger, result, stopwatch);
            return doExecuteOp(writer).addEventListener(completionListener).voided();
        }

        abstract Future<WriteResponse> doExecuteOp(AsyncLogWriter writer);
        
        @Override 
        void fail(ResponseHeader header) {
            result.setValue(writeResponse(header)); 
        }
    }

    static final byte[] HEARTBEAT_DATA = "heartbeat".getBytes(UTF_8);

    class HeartbeatOp extends AbstractWriteOp {

        boolean writeControlRecord = false;

        HeartbeatOp(String stream, WriteContext context) {
            super(stream, context, heartbeatStat);
        }

        HeartbeatOp setWriteControlRecord(boolean writeControlRecord) {
            this.writeControlRecord = writeControlRecord;
            return this;
        }

        @Override
        Future<WriteResponse> doExecuteOp(AsyncLogWriter writer) {
            // write a control record if heartbeat is the first request of the recovered log segment.
            if (writeControlRecord) {
                long txnId;
                Future<DLSN> writeResult;
                synchronized (txnLock) {
                    txnId = System.currentTimeMillis();
                    writeResult = ((BKUnPartitionedAsyncLogWriter) writer).writeControlRecord(new LogRecord(txnId, HEARTBEAT_DATA));
                }
                return writeResult.map(new AbstractFunction1<DLSN, WriteResponse>() {
                    @Override
                    public WriteResponse apply(DLSN value) {
                        return writeResponse(successResponseHeader()).setDlsn(value.serialize(dlsnVersion));
                    }
                });
            } else {
                return Future.value(writeResponse(successResponseHeader()));
            }
        }
    }

    class TruncateOp extends AbstractWriteOp {

        final DLSN dlsn;

        TruncateOp(String stream, DLSN dlsn, WriteContext context) {
            super(stream, context, truncateStat);
            this.dlsn = dlsn;
        }

        @Override
        Future<WriteResponse> doExecuteOp(AsyncLogWriter writer) {
            if (!stream.equals(writer.getStreamName())) {
                logger.error("Truncate: Stream Name Mismatch in the Stream Map {}, {}", stream, writer.getStreamName());
                return Future.exception(new IllegalStateException("The stream mapping is incorrect, fail the request"));
            }
            return writer.truncate(dlsn).map(new AbstractFunction1<Boolean, WriteResponse>() {
                @Override
                public WriteResponse apply(Boolean v1) {
                    return writeResponse(successResponseHeader());
                }
            });
        }
    }

    class DeleteOp extends AbstractWriteOp implements Runnable {

        final Promise<WriteResponse> deletePromise =
                new Promise<WriteResponse>();

        DeleteOp(String stream, WriteContext context) {
            super(stream, context, deleteStat);
        }

        @Override
        Future<WriteResponse> doExecuteOp(AsyncLogWriter writer) {
            if (null == schedule(this, 0)) {
                return Future.exception(new ServiceUnavailableException("Couldn't schedule a delete task."));
            }
            return deletePromise;
        }

        @Override
        public void run() {
            Stream s = streams.get(stream);
            if (null == s) {
                logger.warn("No stream {} to delete.", stream);
                deletePromise.setException(new UnexpectedException("No stream " + stream + " to delete."));
            } else {
                logger.info("Deleting stream {}, {}", stream, s);
                try {
                    s.delete();
                    logger.info("Removed stream {}.", stream);
                    acquiredStreams.remove(stream, s);
                    if (streams.remove(stream, s)) {
                        s.close();
                    }
                    deletePromise.setValue(writeResponse(successResponseHeader()));
                } catch (IOException e) {
                    logger.error("Failed on removing stream {} : ", stream, e);
                    deletePromise.setException(e);
                }
            }
        }
    }

    class ReleaseOp extends AbstractWriteOp implements Runnable {

        final Promise<WriteResponse> releasePromise =
                new Promise<WriteResponse>();

        ReleaseOp(String stream, WriteContext context) {
            super(stream, context, releaseStat);
        }

        @Override
        Future<WriteResponse> doExecuteOp(AsyncLogWriter writer) {
            if (null == schedule(this, 0)) {
                return Future.exception(new ServiceUnavailableException("Couldn't schedule a release task."));
            }
            return releasePromise;
        }

        @Override
        public void run() {
            Stream s = streams.remove(stream);
            if (null == s) {
                logger.warn("No stream {} to release.", stream);
            } else {
                logger.info("Releasing ownership for stream {}, {}", stream, s);
                acquiredStreams.remove(stream, s);
                s.close();
            }
            releasePromise.setValue(writeResponse(successResponseHeader()));
        }
    }

    class WriteOp extends AbstractWriteOp {
        final byte[] payload;

        WriteOp(String stream, ByteBuffer data, WriteContext context) {
            super(stream, context, writeStat);
            payload = new byte[data.remaining()];
            data.get(payload);
        }

        @Override
        Future<WriteResponse> doExecuteOp(AsyncLogWriter writer) {
            if (!stream.equals(writer.getStreamName())) {
                logger.error("Write: Stream Name Mismatch in the Stream Map {}, {}", stream, writer.getStreamName());
                return Future.exception(new IllegalStateException("The stream mapping is incorrect, fail the request"));
            }

            long txnId;
            Future<DLSN> writeResult;
            synchronized (txnLock) {
                txnId = nextTxId();
                writeResult = writer.write(new LogRecord(txnId, payload));
            }
            if (serverMode.equals(ServerMode.DURABLE)) {
                return writeResult.map(new AbstractFunction1<DLSN, WriteResponse>() {
                    @Override
                    public WriteResponse apply(DLSN value) {
                        return writeResponse(successResponseHeader()).setDlsn(value.serialize(dlsnVersion));
                    }
                });
            } else {
                final Promise<WriteResponse> writePromise = new Promise<WriteResponse>();
                if (delayMs > 0) {
                    final long txnIdToReturn = txnId;
                    try {
                        executorService.schedule(new Runnable() {
                            @Override
                            public void run() {
                                opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                                writePromise.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                            }
                        }, delayMs, TimeUnit.MILLISECONDS);
                    } catch (RejectedExecutionException ree) {
                        opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                        writePromise.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                    }
                } else {
                    opStatsLogger.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                    writePromise.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnId)));
                }
                return writePromise;
            }
        }
    }

    class Stream extends Thread {
        final String name;

        // lock
        final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

        DistributedLogManager manager;

        volatile AsyncLogWriter writer;
        volatile StreamStatus status;
        volatile String owner;
        volatile Throwable lastException;
        volatile boolean running = true;
        private volatile Queue<StreamOp> pendingOps = new ArrayDeque<StreamOp>();

        final Object streamLock = new Object();
        // last acquire time
        final Stopwatch lastAcquireWatch = new Stopwatch();
        // last acquire failure time
        final Stopwatch lastAcquireFailureWatch = new Stopwatch();
        long nextAcquireWaitTimeMs;

        // Stats
        private StatsLogger exceptionStatLogger;

        Stream(String name) {
            super("Stream-" + name);
            this.name = name;
            status = StreamStatus.UNINITIALIZED;
        }

        public Stream initialize() throws IOException {
            this.manager = dlFactory.createDistributedLogManagerWithSharedClients(name);
            status = StreamStatus.INITIALIZING;
            lastException = new IOException("Fail to write record to stream " + name);
            nextAcquireWaitTimeMs = dlConfig.getZKSessionTimeoutMilliseconds() * 3 / 5;
            // expose stream status
            StatsLogger streamLogger = perStreamStatLogger.scope(name);
            streamLogger.registerGauge("stream_status", new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return StreamStatus.UNINITIALIZED.getCode();
                }

                @Override
                public Number getSample() {
                    return status.getCode();
                }
            });
            exceptionStatLogger = streamLogger.scope("exceptions");
            return this;
        }

        @Override
        public String toString() {
            return String.format("Stream:%s, %s, %s Status:%s", name, manager, writer, status);
        }

        @Override
        public void run() {
            boolean shouldClose = false;
            while (running) {
                boolean needAcquire = false;
                synchronized (this) {
                    switch (this.status) {
                    case INITIALIZING:
                        acquiredStreams.remove(name, this);
                        needAcquire = true;
                        break;
                    case FAILED:
                        this.status = StreamStatus.INITIALIZING;
                        acquiredStreams.remove(name, this);
                        needAcquire = true;
                        break;
                    case BACKOFF:
                        // there are pending ops, try re-acquiring the stream again
                        if (pendingOps.size() > 0) {
                            this.status = StreamStatus.INITIALIZING;
                            acquiredStreams.remove(name, this);
                            needAcquire = true;
                        }
                        break;
                    default:
                        break;
                    }
                }
                if (needAcquire) {
                    lastAcquireWatch.reset().start();
                    acquireStream();
                } else if (StreamStatus.INITIALIZED != status && lastAcquireWatch.elapsed(TimeUnit.HOURS) > 2) {
                    if (streams.remove(name, this)) {
                        logger.info("Removed acquired stream {} -> writer {}", name, this);
                        shouldClose = true;
                        break;
                    }
                }
                try {
                    synchronized (streamLock) {
                        streamLock.wait(nextAcquireWaitTimeMs);
                    }
                } catch (InterruptedException e) {
                    // interrupted
                }
            }
            if (shouldClose) {
                requestClose(null);
            }
        }

        /**
         * Wait for stream being re-acquired if needed. Otherwise, execute the <i>op</i> immediately.
         *
         * @param op
         *          stream operation to execute.
         */
        void waitIfNeededAndWrite(StreamOp op) {
            boolean completeOpNow = false;
            boolean success = true;
            if (StreamStatus.CLOSED == status) {
                // Stream is closed, fail the op immediately
                op.fail(StatusCode.STREAM_UNAVAILABLE, "Stream " + name + " is closed.");
                return;
            } if (StreamStatus.INITIALIZED == status && writer != null) {
                completeOpNow = true;
                success = true;
            } else {
                synchronized (this) {
                    if (StreamStatus.CLOSED == status) {
                        // complete the write op as {@link #executeOp(op, success)} will handle closed case.
                        completeOpNow = true;
                        success = true;
                    } if (StreamStatus.INITIALIZED == status) {
                        completeOpNow = true;
                        success = true;
                    } else if (StreamStatus.BACKOFF == status &&
                            lastAcquireFailureWatch.elapsed(TimeUnit.MILLISECONDS) < nextAcquireWaitTimeMs) {
                        completeOpNow = true;
                        success = false;
                    } else { // closing & initializing
                        pendingOps.add(op);
                        if (1 == pendingOps.size()) {
                            if (op instanceof HeartbeatOp) {
                                ((HeartbeatOp) op).setWriteControlRecord(true);
                            }
                            // when any op is added in pending queue, notify stream to acquire the stream.
                            synchronized (streamLock) {
                                streamLock.notifyAll();
                            }
                        }
                    }
                }
            }
            if (completeOpNow) {
                executeOp(op, success);
            }
        }

        /**
         * Execute the <i>op</i> immediately.
         *
         * @param op
         *          stream operation to execute.
         * @param success
         *          whether the operation is success or not.
         */
        void executeOp(StreamOp op, boolean success) {
            closeLock.readLock().lock();
            try {
                if (StreamStatus.CLOSED == status) {
                    op.fail(StatusCode.STREAM_UNAVAILABLE, "Stream " + name + " is closed.");
                    return;
                }
                doExecuteOp(op, success);
            } finally {
                closeLock.readLock().unlock();
            }
        }

        private void doExecuteOp(final StreamOp op, boolean success) {
            AsyncLogWriter w = writer;
            if (null != w && success) {
                op.execute(w).addEventListener(new FutureEventListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        // nop
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        countException(cause, exceptionStatLogger);
                        if (cause instanceof DLException) {
                            final DLException dle = (DLException) cause;
                            switch (dle.getCode()) {
                            case FOUND:
                                handleOwnershipAcquireFailedException(op, (OwnershipAcquireFailedException) cause);
                                break;
                            case ALREADY_CLOSED:
                                op.fail(null, cause);
                                handleAlreadyClosedException((AlreadyClosedException) cause);
                                break;
                            // exceptions that mostly from client (e.g. too large record)
                            case NOT_IMPLEMENTED:
                            case METADATA_EXCEPTION:
                            case LOG_EMPTY:
                            case LOG_NOT_FOUND:
                            case TRUNCATED_TRANSACTION:
                            case END_OF_STREAM:
                            case TRANSACTION_OUT_OF_ORDER:
                            case INVALID_STREAM_NAME:
                                op.fail(null, cause);
                                break;
                            // exceptions that *could* / *might* be recovered by creating a new writer
                            default:
                                handleRecoverableDLException(op, cause);
                                break;
                            }
                        } else {
                            handleUnknownException(op, cause);
                        }
                    }
                });
            } else {
                failOp(op, owner, lastException);
            }
        }

        private void failOp(StreamOp op, String owner, Throwable t) {
            op.fail(owner, t);
        }

        /**
         * Handle recoverable dl exception.
         *
         * @param op
         *          stream operation executing
         * @param cause
         *          exception received when executing <i>op</i>
         */
        private void handleRecoverableDLException(StreamOp op, final Throwable cause) {
            AsyncLogWriter oldWriter = null;
            boolean statusChanged = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    oldWriter = setStreamStatus(StreamStatus.FAILED, null, null, cause);
                    statusChanged = true;
                }
            }
            if (statusChanged) {
                abort(oldWriter);
                logger.error("Failed to write data into stream {} : ", name, cause);
            }
            failOp(op, null, cause);
        }

        /**
         * Handle unknown exception when executing <i>op</i>.
         *
         * @param op
         *          stream operation executing
         * @param cause
         *          exception received when executing <i>op</i>
         */
        private void handleUnknownException(StreamOp op, final Throwable cause) {
            AsyncLogWriter oldWriter = null;
            boolean statusChanged = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    oldWriter = setStreamStatus(StreamStatus.FAILED, null, null, cause);
                    statusChanged = true;
                }
            }
            if (statusChanged) {
                abort(oldWriter);
                logger.error("Failed to write data into stream {} : ", name, cause);
            }
            failOp(op, null, cause);
        }

        /**
         * Handle losing ownership during executing <i>op</i>.
         *
         * @param op
         *          stream operation executing
         * @param oafe
         *          the ownership exception received when executing <i>op</i>
         */
        private void handleOwnershipAcquireFailedException(StreamOp op, final OwnershipAcquireFailedException oafe) {
            logger.error("Failed to write data into stream {} : ", name, oafe);
            AsyncLogWriter oldWriter = null;
            boolean statusChanged = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    oldWriter =
                        setStreamStatus(StreamStatus.BACKOFF, null, oafe.getCurrentOwner(), oafe);
                    statusChanged = true;
                }
            }
            if (statusChanged) {
                abort(oldWriter);
            }
            failOp(op, oafe.getCurrentOwner(), oafe);
        }

        /**
         * Handling already closed exception.
         */
        private void handleAlreadyClosedException(AlreadyClosedException ace) {
            // TODO: change to more specific exception: e.g. bkzk session expired?
            logger.error("DL Manager is closed. Quiting : ", ace);
            Runtime.getRuntime().exit(-1);
        }

        void acquireStream() {
            Queue<StreamOp> oldPendingOps;
            boolean success;
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                AsyncLogWriter w = manager.startAsyncLogSegmentNonPartitioned();
                synchronized (this) {
                    setStreamStatus(StreamStatus.INITIALIZED, w, null, null);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = true;
                }
            } catch (final AlreadyClosedException ace) {
                countException(ace, exceptionStatLogger);
                handleAlreadyClosedException(ace);
                return;
            } catch (final OwnershipAcquireFailedException oafe) {
                countException(oafe, exceptionStatLogger);
                logger.error("Failed to initialize stream {} : ", name, oafe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.BACKOFF, null, oafe.getCurrentOwner(), oafe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            } catch (final IOException ioe) {
                countException(ioe, exceptionStatLogger);
                logger.error("Failed to initialize stream {} : ", name, ioe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED, null, null, ioe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            }
            if (success) {
                streamAcquireStat.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } else {
                streamAcquireStat.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
            for (StreamOp op : oldPendingOps) {
                executeOp(op, success);
            }
        }

        synchronized AsyncLogWriter setStreamStatus(StreamStatus status, AsyncLogWriter writer,
                                                    String owner, Throwable t) {
            AsyncLogWriter oldWriter = this.writer;
            this.writer = writer;
            if (null != owner && owner.equals(clientId)) {
                // I lost the ownership but left a lock over zookeeper
                // I should not ask client to redirect to myself again as I can't handle it :(
                // shutdown myself
                triggerShutdown();
                this.owner = null;
            } else {
                this.owner = owner;
            }
            this.lastException = t;
            this.status = status;
            if (StreamStatus.BACKOFF == status && null != owner) {
                // start failure watch
                this.lastAcquireFailureWatch.reset().start();
            }
            if (StreamStatus.INITIALIZED == status) {
                acquiredStreams.put(name, this);
                logger.info("Inserted acquired stream {} -> writer {}", name, this);
            } else {
                acquiredStreams.remove(name, this);
                logger.info("Removed acquired stream {} -> writer {}", name, this);
            }
            return oldWriter;
        }

        void close(DistributedLogManager dlm) {
            if (null != dlm) {
                try {
                    dlm.close();
                } catch (IOException ioe) {
                    logger.warn("Failed to close dlm for {} : ", ioe);
                }
            }
        }

        void close(AsyncLogWriter writer) {
            if (null != writer) {
                try {
                    writer.close();
                } catch (IOException ioe) {
                    logger.warn("Failed to close async log writer for {} : ", ioe);
                }
            }
        }

        void abort(AsyncLogWriter writer) {
            if (null != writer) {
                try {
                    writer.abort();
                } catch (IOException e) {
                    logger.warn("Failed to abort async log writer for {} : ", e);
                }
            }
        }

        void requestClose(final CountDownLatch closeLatch) {
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSING == status) {
                    return;
                }
                status = StreamStatus.CLOSING;
                acquiredStreams.remove(name, this);
            } finally {
                closeLock.writeLock().unlock();
            }
            // we will fail the requests that are coming in between closing and closed only
            // after the async writer is closed. so we could clear up the lock before redirect
            // them.
            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        close();
                        if (null != closeLatch) {
                            closeLatch.countDown();
                        }
                    }
                });
            } catch (RejectedExecutionException ree) {
                logger.warn("Failed to schedule closing stream {}, close it directly : ", name, ree);
                close();
                if (null != closeLatch) {
                    closeLatch.countDown();
                }
            }
        }

        void delete() throws IOException {
            if (null != writer) {
                writer.close();
                writer = null;
            }
            if (null == manager) {
                throw new UnexpectedException("No stream " + name + " to delete");
            }
            try {
                manager.delete();
            } finally {
                close();
            }
        }

        void close() {
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSED == status) {
                    return;
                }
                status = StreamStatus.CLOSED;
                acquiredStreams.remove(name, this);
            } finally {
                closeLock.writeLock().unlock();
            }
            logger.info("Closing stream {} ...", name);
            running = false;
            // stop any outstanding ownership acquire actions first
            interrupt();
            try {
                join();
            } catch (InterruptedException e) {
                logger.error("Interrupted on closing stream {} : ", name, e);
            }
            logger.info("Stopped acquiring ownership for stream {}.", name);
            // Close the writers to release the locks before failing the requests
            close(writer);
            close(manager);
            // Failed the pending requests.
            Queue<StreamOp> oldPendingOps;
            synchronized (this) {
                oldPendingOps = pendingOps;
                pendingOps = new ArrayDeque<StreamOp>();
            }
            for (StreamOp op : oldPendingOps) {
                op.fail(StatusCode.STREAM_UNAVAILABLE, "Stream " + name + " is closed.");
            }
            logger.info("Closed stream {}.", name);
        }
    }

    private final DistributedLogConfiguration dlConfig;
    private final DistributedLogManagerFactory dlFactory;
    private final ConcurrentHashMap<String, Stream> streams =
            new ConcurrentHashMap<String, Stream>();
    private final ConcurrentHashMap<String, Stream> acquiredStreams =
            new ConcurrentHashMap<String, Stream>();

    private boolean closed = false;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final CountDownLatch keepAliveLatch;
    private final Object txnLock = new Object();

    // Stats
    // operation stats
    private final OpStatsLogger bulkWriteStat;
    private final OpStatsLogger writeStat;
    private final OpStatsLogger heartbeatStat;
    private final OpStatsLogger truncateStat;
    private final OpStatsLogger deleteStat;
    private final OpStatsLogger releaseStat;
    private final Counter redirects;
    // exception stats
    private final StatsLogger exceptionStatLogger;
    private final ConcurrentHashMap<String, Counter> exceptionCounters =
            new ConcurrentHashMap<String, Counter>();
    private final StatsLogger statusCodeStatLogger;
    private final ConcurrentHashMap<StatusCode, Counter> statusCodeCounters =
            new ConcurrentHashMap<StatusCode, Counter>();
    // streams stats
    private final OpStatsLogger streamAcquireStat;
    // per streams stats
    private final StatsLogger perStreamStatLogger;

    private final byte dlsnVersion;
    private final String clientId;
    private final ServerMode serverMode;
    private final ScheduledExecutorService executorService;
    private final long delayMs;

    DistributedLogServiceImpl(DistributedLogConfiguration dlConf, URI uri, StatsLogger statsLogger,
                              CountDownLatch keepAliveLatch)
            throws IOException {
        // Configuration.
        this.dlConfig = dlConf;
        // by default, we generate version0 dlsn for backward compatability. the proxy server
        // would update to generate version1 dlsn after all clients updates to use latest version.
        this.dlsnVersion = dlConf.getByte("server_dlsn_version", DLSN.VERSION0);
        this.serverMode = ServerMode.valueOf(dlConf.getString("server_mode", ServerMode.DURABLE.toString()));
        int serverRegionId = dlConf.getInt("server_region_id", DistributedLogConstants.LOCAL_REGION_ID);
        int serverPort = dlConf.getInt("server_port", 0);
        int shard = dlConf.getInt("server_shard", -1);
        int numThreads = dlConf.getInt("server_threads", Runtime.getRuntime().availableProcessors());
        this.clientId = DLSocketAddress.toLockId(DLSocketAddress.getSocketAddress(serverPort), shard);
        String allocatorPoolName = String.format("allocator_%04d_%010d", serverRegionId, shard);
        dlConf.setLedgerAllocatorPoolName(allocatorPoolName);
        this.dlFactory = new DistributedLogManagerFactory(dlConf, uri, statsLogger, clientId, serverRegionId);
        this.keepAliveLatch = keepAliveLatch;

        // Executor Service.
        this.executorService = Executors.newScheduledThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Executor-%d").build());
        if (ServerMode.MEM.equals(serverMode)) {
            this.delayMs = dlConf.getLong("server_latency_delay", 0);
        } else {
            this.delayMs = 0;
        }

        // status about server health
        statsLogger.registerGauge("proxy_status", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return closed ? -1 : 1;
            }
        });

        // Stats on requests
        StatsLogger requestsStatsLogger = statsLogger.scope("request");
        this.bulkWriteStat = requestsStatsLogger.getOpStatsLogger("bulkWrite");
        this.writeStat = requestsStatsLogger.getOpStatsLogger("write");
        this.heartbeatStat = requestsStatsLogger.getOpStatsLogger("heartbeat");
        this.truncateStat = requestsStatsLogger.getOpStatsLogger("truncate");
        this.deleteStat = requestsStatsLogger.getOpStatsLogger("delete");
        this.releaseStat = requestsStatsLogger.getOpStatsLogger("release");
        this.redirects = requestsStatsLogger.getCounter("redirect");
        this.exceptionStatLogger = requestsStatsLogger.scope("exceptions");
        this.statusCodeStatLogger = requestsStatsLogger.scope("statuscode");

        // Stats on streams
        StatsLogger streamsStatsLogger = statsLogger.scope("streams");
        streamsStatsLogger.registerGauge("acquired", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return acquiredStreams.size();
            }
        });
        streamsStatsLogger.registerGauge("cached", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return streams.size();
            }
        });
        streamAcquireStat = streamsStatsLogger.getOpStatsLogger("acquire");
        // Stats on per streams
        boolean enablePerStreamStat = dlConf.getBoolean("server_enable_perstream_stat", true);
        perStreamStatLogger = enablePerStreamStat ? statsLogger.scope("perstreams") : NullStatsLogger.INSTANCE;

        logger.info("Running distributedlog server in {} mode, delay ms {}, client id {}, allocator pool {}, perstream stat {}.",
                new Object[] { serverMode, delayMs, clientId, allocatorPoolName, enablePerStreamStat });
    }

    private java.util.concurrent.Future<?> schedule(Runnable r, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return null;
            }
            if (delayMs > 0) {
                return executorService.schedule(r, delayMs, TimeUnit.MILLISECONDS);
            } else {
                return executorService.submit(r);
            }
        } catch (RejectedExecutionException ree) {
            logger.error("Failed to schedule task {} in {} ms : ",
                    new Object[] { r, delayMs, ree });
            return null;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    void countException(Throwable t, StatsLogger streamExceptionLogger) {
        String exceptionName = null == t ? "null" : t.getClass().getName();
        Counter counter = exceptionCounters.get(exceptionName);
        if (null == counter) {
            counter = exceptionStatLogger.getCounter(exceptionName);
            Counter oldCounter = exceptionCounters.putIfAbsent(exceptionName, counter);
            if (null != oldCounter) {
                counter = oldCounter;
            }
        }
        counter.inc();
        streamExceptionLogger.getCounter(exceptionName).inc();
    }

    void countStatusCode(StatusCode code) {
        Counter counter = statusCodeCounters.get(code);
        if (null == counter) {
            counter = statusCodeStatLogger.getCounter(code.name());
            Counter oldCounter = statusCodeCounters.putIfAbsent(code, counter);
            if (null != oldCounter) {
                counter = oldCounter;
            }
        }
        counter.inc();
    }

    @Override
    public Future<ServerInfo> handshake() {
        ServerInfo serverInfo = new ServerInfo();
        Map<String, String> ownerships = new HashMap<String, String>();
        for (String name : acquiredStreams.keySet()) {
            Stream stream = acquiredStreams.get(name);
            if (null == stream) {
                continue;
            }
            String owner = stream.owner;
            if (null == owner) {
                ownerships.put(name, clientId);
            }
        }
        serverInfo.setOwnerships(ownerships);
        return Future.value(serverInfo);
    }

    Stream getLogWriter(String stream) throws IOException {
        Stream writer = streams.get(stream);
        if (null == writer) {
            closeLock.readLock().lock();
            try {
                // if it is closed, we would not acquire stream again.
                if (closed) {
                    return null;
                }
                writer = new Stream(stream);
                Stream oldWriter = streams.putIfAbsent(stream, writer);
                if (null != oldWriter) {
                    writer = oldWriter;
                } else {
                    logger.info("Inserted mapping stream {} -> writer {}", stream, writer);
                    writer.initialize().start();
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return writer;
    }

    // Service interface methods

    @Override
    public Future<WriteResponse> write(final String stream, ByteBuffer data) {
        return doWrite(stream, data, new WriteContext());
    }

    @Override
    public Future<BulkWriteResponse> writeBulkWithContext(final String stream, List<ByteBuffer> data, WriteContext ctx) {
        BulkWriteOp op = new BulkWriteOp(stream, data, ctx);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<WriteResponse> writeWithContext(final String stream, ByteBuffer data, WriteContext ctx) {
        return doWrite(stream, data, ctx);
    }

    @Override
    public Future<WriteResponse> heartbeat(String stream, WriteContext ctx) {
        HeartbeatOp op = new HeartbeatOp(stream, ctx);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<WriteResponse> truncate(String stream, String dlsn, WriteContext ctx) {
        TruncateOp op = new TruncateOp(stream, DLSN.deserialize(dlsn), ctx);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<WriteResponse> delete(String stream, WriteContext ctx) {
        DeleteOp op = new DeleteOp(stream, ctx);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<WriteResponse> release(String stream, WriteContext ctx) {
        ReleaseOp op = new ReleaseOp(stream, ctx);
        doExecuteStreamOp(op);
        return op.result;
    }

    private Future<WriteResponse> doWrite(final String name, ByteBuffer data, WriteContext ctx) {
        WriteOp op = new WriteOp(name, data, ctx);
        doExecuteStreamOp(op);
        return op.result;
    }

    private void doExecuteStreamOp(StreamOp op) {
        Stream stream;
        try {
            stream = getLogWriter(op.stream);
        } catch (IOException e) {
            op.fail(null, e);
            return;
        }
        if (null == stream) {
            // redirect the requests when stream is unavailable.
            op.fail(StatusCode.SERVICE_UNAVAILABLE, "Server " + clientId + " is closed.");
            return;
        }
        stream.waitIfNeededAndWrite(op);
    }

    void shutdown() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
        int numAcquired = acquiredStreams.size();
        int numCached = streams.size();
        logger.info("Closing all acquired streams : acquired = {}, cached = {}.",
                    numAcquired, numCached);
        Set<Stream> streamsToClose = new HashSet<Stream>();
        streamsToClose.addAll(streams.values());
        CountDownLatch closeLatch = new CountDownLatch(streamsToClose.size());
        for (Stream stream: streamsToClose) {
            if (streams.remove(stream.name, stream)) {
                stream.requestClose(closeLatch);
            } else {
                closeLatch.countDown();
            }
        }

        logger.info("Waiting for closing all streams ...");
        try {
            if (closeLatch.await(5, TimeUnit.MINUTES)) {
                logger.info("Closed all streams.");
            } else {
                logger.warn("Sorry, we didn't close all streams gracefully in 5 minutes.");
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted on waiting for closing all streams : ", e);
        }

        // shutdown the executor after requesting closing streams.
        SchedulerUtils.shutdownScheduler(executorService, 60, TimeUnit.SECONDS);
        // shutdown the dl factory
        logger.info("Closing distributedlog factory ...");
        dlFactory.close();
        logger.info("Closed distributedlog factory.");

        // release the keepAliveLatch in case shutdown is called from a shutdown hook.
        keepAliveLatch.countDown();
        logger.info("Finished shutting down distributedlog service.");
    }

    void triggerShutdown() {
        // release the keepAliveLatch to let the main thread shutdown the whole service.
        logger.info("Releasing KeepAlive Latch to trigger shutdown ...");
        keepAliveLatch.countDown();
        logger.info("Released KeepAlive Latch. Main thread will shut the service down.");
    }
}
