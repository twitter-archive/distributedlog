package com.twitter.distributedlog.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.BKUnPartitionedAsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.DynamicConfigurationFactory;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.RegionUnavailableException;
import com.twitter.distributedlog.exceptions.ServiceUnavailableException;
import com.twitter.distributedlog.exceptions.StreamNotReadyException;
import com.twitter.distributedlog.exceptions.StreamUnavailableException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.feature.AbstractFeatureProvider;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.ClientInfo;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.HeartbeatOptions;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.util.Await;
import com.twitter.util.ConstFuture;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.Future$;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Try;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration.ConfigurationException;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import static com.google.common.base.Charsets.UTF_8;

class DistributedLogServiceImpl implements DistributedLogService.ServiceIface, StreamManager {

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
        CLOSED(-5),
        // if a stream is in error state, it should be abort during closing.
        ERROR(-6);

        final int code;

        StreamStatus(int code) {
            this.code = code;
        }

        int getCode() {
            return code;
        }

        static boolean isUnavailable(StreamStatus status) {
            return StreamStatus.ERROR == status || StreamStatus.CLOSING == status || StreamStatus.CLOSED == status;
        }
    }

    static enum ServerMode {
        DURABLE,
        MEM
    }

    static ResponseHeader operationDeniedResponseHeader() {
        return new ResponseHeader(StatusCode.REQUEST_DENIED);
    }

    static ResponseHeader ownerToResponseHeader(String owner) {
        return new ResponseHeader(StatusCode.FOUND).setLocation(owner);
    }

    static ResponseHeader successResponseHeader() {
        return new ResponseHeader(StatusCode.SUCCESS);
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

    // WriteResponse
    static WriteResponse writeResponse(ResponseHeader responseHeader) {
        return new WriteResponse(responseHeader);
    }

    // BullkWriteResponse
    static BulkWriteResponse bulkWriteResponse(ResponseHeader responseHeader) {
        return new BulkWriteResponse(responseHeader);
    }

    static interface WriteOpWithPayload {
      // Return the payload size in bytes
      public long getPayloadSize();
    }

    static interface StreamOp {
        Future<Void> execute(AsyncLogWriter writer);
        Future<ResponseHeader> responseHeader();
        void fail(Throwable t);
        String streamName();
        Stopwatch stopwatch();
    }

    static abstract class AbstractStreamOp<Response> implements StreamOp {
        protected final String stream;
        protected final OpStatsLogger opStatsLogger;
        protected final Promise<Response> result = new Promise<Response>();
        protected final Stopwatch stopwatch = Stopwatch.createUnstarted();

        AbstractStreamOp(String stream, OpStatsLogger statsLogger) {
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
                fail(ownerToResponseHeader(oafe.getCurrentOwner()));
            } else {
                opStatsLogger.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                fail(exceptionToResponseHeader(cause));
            }
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
            return statsLogger.scope("request").getOpStatsLogger(opName);
        }
    }

    static class BulkWriteOp extends AbstractStreamOp<BulkWriteResponse> implements WriteOpWithPayload {
        private final List<ByteBuffer> buffers;
        private final long payloadSize;

        // Record stats
        private final Counter successRecordCounter;
        private final Counter failureRecordCounter;
        private final Counter redirectRecordCounter;

        // We need to pass these through to preserve ownership change behavior in
        // client/server. Only include failures which are guaranteed to have failed
        // all subsequent writes.
        private boolean isDefiniteFailure(Try<DLSN> result) {
            boolean def = false;
            try {
                result.get();
            } catch (Exception ex) {
                if (ex instanceof OwnershipAcquireFailedException ||
                    ex instanceof AlreadyClosedException ||
                    ex instanceof LockingException) {
                    def = true;
                }
            }
            return def;
        }

        BulkWriteOp(String stream, List<ByteBuffer> buffers, StatsLogger statsLogger) {
            super(stream, requestStat(statsLogger, "bulkWrite"));
            this.buffers = buffers;
            long total = 0;
            // We do this here because the bytebuffers are mutable.
            for (ByteBuffer bb : buffers) {
              total += bb.remaining();
            }
            this.payloadSize = total;

            // Write record stats
            StatsLogger recordsStatsLogger = statsLogger.scope("records");
            this.successRecordCounter = recordsStatsLogger.getCounter("success");
            this.failureRecordCounter = recordsStatsLogger.getCounter("failure");
            this.redirectRecordCounter = recordsStatsLogger.getCounter("redirect");
        }

        @Override
        public long getPayloadSize() {
          return payloadSize;
        }

        @Override
        protected Future<BulkWriteResponse> executeOp(AsyncLogWriter writer) {

            // Need to convert input buffers to LogRecords.
            List<LogRecord> records = asRecordList(buffers);
            Future<List<Future<DLSN>>> futureList = writer.writeBulk(records);

            // Collect into a list of tries to make it easier to extract exception or DLSN.
            Future<List<Try<DLSN>>> writes = asTryList(futureList);

            Future<BulkWriteResponse> response = writes.flatMap(
                new AbstractFunction1<List<Try<DLSN>>, Future<BulkWriteResponse>>() {
                    @Override
                    public Future<BulkWriteResponse> apply(List<Try<DLSN>> results) {

                        // Considered a success at batch level even if no individual writes succeeed.
                        // The reason is that its impossible to make an appropriate decision re retries without
                        // individual buffer failure reasons.
                        List<WriteResponse> writeResponses = new ArrayList<WriteResponse>(results.size());
                        BulkWriteResponse bulkWriteResponse =
                            bulkWriteResponse(successResponseHeader()).setWriteResponses(writeResponses);

                        // Promote the first result to an op-level failure if we're sure all other writes have
                        // failed.
                        if (results.size() > 0) {
                            Try<DLSN> firstResult = results.get(0);
                            if (isDefiniteFailure(firstResult)) {
                                return new ConstFuture(firstResult);
                            }
                        }

                        // Translate all futures to write responses.
                        Iterator<Try<DLSN>> iterator = results.iterator();
                        while (iterator.hasNext()) {
                            Try<DLSN> completedFuture = iterator.next();
                            try {
                                DLSN dlsn = completedFuture.get();
                                WriteResponse writeResponse = new WriteResponse(successResponseHeader()).setDlsn(dlsn.serialize());
                                writeResponses.add(writeResponse);
                                successRecordCounter.inc();
                            } catch (Exception ioe) {
                                WriteResponse writeResponse = new WriteResponse(exceptionToResponseHeader(ioe));
                                writeResponses.add(writeResponse);
                                if (StatusCode.FOUND == writeResponse.getHeader().getCode()) {
                                    redirectRecordCounter.inc();
                                } else {
                                    failureRecordCounter.inc();
                                }
                            }
                        }

                        return Future.value(bulkWriteResponse);
                    }
                }
            );

            return response;
        }

        private List<LogRecord> asRecordList(List<ByteBuffer> buffers) {
            List<LogRecord> records = new ArrayList<LogRecord>(buffers.size());
            for (ByteBuffer buffer : buffers) {
                byte[] payload = new byte[buffer.remaining()];
                buffer.get(payload);
                records.add(new LogRecord(nextTxId(), payload));
            }
            return records;
        }

        private Future<List<Try<DLSN>>> asTryList(Future<List<Future<DLSN>>> futureList) {
            return futureList.flatMap(new AbstractFunction1<List<Future<DLSN>>, Future<List<Try<DLSN>>>>() {
                @Override
                public Future<List<Try<DLSN>>> apply(List<Future<DLSN>> results) {
                    return Future$.MODULE$.collectToTry(results);
                }
            });
        }

        @Override
        protected void fail(ResponseHeader header) {
            if (StatusCode.FOUND == header.getCode()) {
                redirectRecordCounter.add(buffers.size());
            } else {
                failureRecordCounter.add(buffers.size());
            }
            result.setValue(bulkWriteResponse(header));
        }

        @Override
        public Future<ResponseHeader> responseHeader() {
            return result.map(new AbstractFunction1<BulkWriteResponse, ResponseHeader>() {
                @Override
                public ResponseHeader apply(BulkWriteResponse response) {
                    return response.getHeader();
                }
            });
        }
    }

    abstract static class AbstractWriteOp extends AbstractStreamOp<WriteResponse> {

        AbstractWriteOp(String stream, OpStatsLogger statsLogger) {
            super(stream, statsLogger);
        }

        @Override
        protected void fail(ResponseHeader header) {
            result.setValue(writeResponse(header));
        }

        Future<WriteResponse> getResult() {
            return result;
        }

        @Override
        public Future<ResponseHeader> responseHeader() {
            return result.map(new AbstractFunction1<WriteResponse, ResponseHeader>() {
                @Override
                public ResponseHeader apply(WriteResponse response) {
                    return response.getHeader();
                }
            });
        }
    }

    static class HeartbeatOp extends AbstractWriteOp {

        static final byte[] HEARTBEAT_DATA = "heartbeat".getBytes(UTF_8);

        private boolean writeControlRecord = false;
        private Object txnLock;
        private byte dlsnVersion;

        HeartbeatOp(String stream, StatsLogger statsLogger, Object txnLock, byte dlsnVersion) {
            super(stream, requestStat(statsLogger, "heartbeat"));
            this.txnLock = txnLock;
            this.dlsnVersion = dlsnVersion;
        }

        HeartbeatOp setWriteControlRecord(boolean writeControlRecord) {
            this.writeControlRecord = writeControlRecord;
            return this;
        }

        @Override
        protected Future<WriteResponse> executeOp(AsyncLogWriter writer) {
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

        TruncateOp(String stream, DLSN dlsn, StatsLogger statsLogger) {
            super(stream, requestStat(statsLogger, "truncate"));
            this.dlsn = dlsn;
        }

        @Override
        protected Future<WriteResponse> executeOp(AsyncLogWriter writer) {
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

    public Future<Void> doDeleteAndRemoveAsync(final String stream) {
        Stream s = streams.get(stream);
        if (null == s) {
            logger.warn("No stream {} to delete.", stream);
            return Future.exception(new UnexpectedException("No stream " + stream + " to delete."));
        } else {
            Future<Void> result;
            logger.info("Deleting stream {}, {}", stream, s);
            try {
                s.delete();
                result = s.requestClose("Stream Deleted");
            } catch (IOException e) {
                logger.error("Failed on removing stream {} : ", stream, e);
                result = Future.exception(e);
            }
            return result;
        }
    }

    /**
     * Must be enqueued to an executor to avoid deadlocks (close and execute-op both
     * try to acquire the same read-write lock).
     */
    @Override
    public Future<Void> deleteAndRemoveAsync(final String stream) {
        final Promise<Void> result = new Promise<Void>();
        java.util.concurrent.Future<?> scheduleFuture = schedule(new Runnable() {
            @Override
            public void run() {
                result.become(doDeleteAndRemoveAsync(stream));
            }
        }, 0);
        if (null == scheduleFuture) {
            return Future.exception(
                    new ServiceUnavailableException("Couldn't schedule a delete task."));
        }
        return result;
    }

    static class DeleteOp extends AbstractWriteOp {
        private final StreamManager streamManager;

        DeleteOp(String stream, StatsLogger statsLogger, StreamManager streamManager) {
            super(stream, requestStat(statsLogger, "delete"));
            this.streamManager = streamManager;
        }

        @Override
        protected Future<WriteResponse> executeOp(AsyncLogWriter writer) {
            Future<Void> result = streamManager.deleteAndRemoveAsync(streamName());
            return result.map(new AbstractFunction1<Void, WriteResponse>() {
                @Override
                public WriteResponse apply(Void value) {
                    return writeResponse(successResponseHeader());
                }
            });
        }
    }

    public Future<Void> doCloseAndRemoveAsync(final String stream) {
        Stream s = streams.get(stream);
        if (null == s) {
            logger.info("No stream {} to release.", stream);
            return Future.value(null);
        } else {
            return s.requestClose("release ownership");
        }
    }

    /**
     * Must be enqueued to an executor to avoid deadlocks (close and execute-op both
     * try to acquire the same read-write lock).
     */
    @Override
    public Future<Void> closeAndRemoveAsync(final String stream) {
        final Promise<Void> releasePromise = new Promise<Void>();
        java.util.concurrent.Future<?> scheduleFuture = schedule(new Runnable() {
            @Override
            public void run() {
                releasePromise.become(doCloseAndRemoveAsync(stream));
            }
        }, 0);
        if (null == scheduleFuture) {
            return Future.exception(
                    new ServiceUnavailableException("Couldn't schedule a release task."));
        }
        return releasePromise;
    }

    static class ReleaseOp extends AbstractWriteOp {
        private final StreamManager streamManager;

        ReleaseOp(String stream, StatsLogger statsLogger, StreamManager streamManager) {
            super(stream, requestStat(statsLogger, "release"));
            this.streamManager = streamManager;
        }

        @Override
        protected Future<WriteResponse> executeOp(AsyncLogWriter writer) {
            Future<Void> result = streamManager.closeAndRemoveAsync(streamName());
            return result.map(new AbstractFunction1<Void, WriteResponse>() {
                @Override
                public WriteResponse apply(Void value) {
                    return writeResponse(successResponseHeader());
                }
            });
        }
    }

    WriteOp newWriteOp(String stream, ByteBuffer data) {
        return new WriteOp(stream, data, statsLogger, txnLock, serverMode, executorService,
                dlsnVersion, delayMs);
    }

    static class WriteOp extends AbstractWriteOp implements WriteOpWithPayload {
        final byte[] payload;

        // Record stats
        private final Counter successRecordCounter;
        private final Counter failureRecordCounter;
        private final Counter redirectRecordCounter;

        private final Object txnLock;
        private final ServerMode serverMode;
        private final ScheduledExecutorService executorService;
        private final byte dlsnVersion;
        private final long delayMs;

        WriteOp(String stream, ByteBuffer data, StatsLogger statsLogger,
                Object txnLock, ServerMode serverMode, ScheduledExecutorService executorService,
                byte dlsnVersion, long delayMs) {
            super(stream, requestStat(statsLogger, "write"));
            payload = new byte[data.remaining()];
            data.get(payload);

            // Write record stats
            StatsLogger recordsStatsLogger = statsLogger.scope("records");
            this.successRecordCounter = recordsStatsLogger.getCounter("success");
            this.failureRecordCounter = recordsStatsLogger.getCounter("failure");
            this.redirectRecordCounter = recordsStatsLogger.getCounter("redirect");

            this.txnLock = txnLock;
            this.serverMode = serverMode;
            this.executorService = executorService;
            this.dlsnVersion = dlsnVersion;
            this.delayMs = delayMs;
        }

        @Override
        public long getPayloadSize() {
          return payload.length;
        }

        @Override
        protected Future<WriteResponse> executeOp(AsyncLogWriter writer) {
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
                        successRecordCounter.inc();
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
                                opStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                                writePromise.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                                successRecordCounter.inc();
                            }
                        }, delayMs, TimeUnit.MILLISECONDS);
                    } catch (RejectedExecutionException ree) {
                        opStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                        writePromise.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnIdToReturn)));
                        successRecordCounter.inc();
                    }
                } else {
                    opStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                    writePromise.setValue(writeResponse(successResponseHeader()).setDlsn(Long.toString(txnId)));
                    successRecordCounter.inc();
                }
                return writePromise;
            }
        }

        @Override
        protected void fail(ResponseHeader header) {
            if (StatusCode.FOUND == header.getCode()) {
                redirectRecordCounter.inc();
            } else {
                failureRecordCounter.inc();
            }
            super.fail(header);
        }
    }

    protected class Stream extends Thread {
        final String name;

        // lock
        final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

        DistributedLogManager manager;

        volatile AsyncLogWriter writer;
        volatile StreamStatus status;
        volatile String owner;
        volatile Throwable lastException;
        volatile boolean running = true;
        boolean suspended = false;
        private volatile Queue<StreamOp> pendingOps = new ArrayDeque<StreamOp>();
        private final Promise<Void> closePromise = new Promise<Void>();

        // A write has been attempted since the last stream acquire.
        private volatile boolean writeSinceLastAcquire = false;

        final Object streamLock = new Object();
        // last acquire time
        final Stopwatch lastAcquireWatch = Stopwatch.createUnstarted();
        // last acquire failure time
        final Stopwatch lastAcquireFailureWatch = Stopwatch.createUnstarted();
        final long nextAcquireWaitTimeMs;

        // Stats
        final StatsLogger streamLogger;
        final StatsLogger exceptionStatLogger;

        // Since we may create and discard streams at initialization if there's a race,
        // must not do any expensive intialization here (particularly any locking or
        // significant resource allocation etc.).
        Stream(String name) {
            super("Stream-" + name);
            this.name = name;
            this.status = StreamStatus.UNINITIALIZED;
            this.streamLogger = perStreamStatLogger.scope(name);
            this.exceptionStatLogger = streamLogger.scope("exceptions");
            this.lastException = new IOException("Fail to write record to stream " + name);
            this.nextAcquireWaitTimeMs = dlConfig.getZKSessionTimeoutMilliseconds() * 3 / 5;
        }

        private DistributedLogManager openLog(String name) throws IOException {
            Optional<DistributedLogConfiguration> streamConfig =
                    Optional.absent();
            Optional<DynamicDistributedLogConfiguration> dynamicStreamConfig =
                    streamConfigProvider.getDynamicStreamConfig(name);

            return dlNamespace.openLog(name, streamConfig, dynamicStreamConfig);
        }

        // Expensive initialization, only called once per stream.
        public Stream initialize() throws IOException {
            manager = openLog(name);

            // Better to avoid registering the gauge multiple times, so do this in init
            // which only gets called once.
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

            // Signal initialization is complete, should be last in this method.
            status = StreamStatus.INITIALIZING;
            return this;
        }

        @VisibleForTesting
        Stream suspendAcquiring() {
            synchronized (streamLock) {
                suspended = true;
            }
            return this;
        }

        @VisibleForTesting
        Stream resumeAcquiring() {
            synchronized (streamLock) {
                suspended = false;
                streamLock.notifyAll();
            }
            return this;
        }

        int numPendingOps() {
            Queue<StreamOp> queue = pendingOps;
            return null == queue ? 0 : queue.size();
        }

        StreamStatus getStatus() {
            return status;
        }

        @Override
        public String toString() {
            return String.format("Stream:%s, %s, %s Status:%s", name, manager, writer, status);
        }

        @Override
        public void run() {
            try {
                boolean shouldClose = false;
                String closeReason = "";
                while (running) {
                    synchronized (streamLock) {
                        while (suspended && running) {
                            try {
                                streamLock.wait();
                            } catch (InterruptedException ie) {
                                // interrupted on waiting
                            }
                        }
                    }

                    if (!running) {
                        break;
                    }

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
                            // We may end up here after timeout on streamLock. To avoid acquire on every timeout
                            // we should only try again if a write has been attempted since the last acquire
                            // attempt. If we end up here because the request handler woke us up, the flag will
                            // be set and we will try to acquire as intended.
                            if (writeSinceLastAcquire) {
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
                    } else if (StreamStatus.isUnavailable(status)) {
                        // if the stream is unavailable, stop the thread and close the stream
                        shouldClose = true;
                        closeReason = "Stream is unavailable anymore";
                        break;
                    } else if (StreamStatus.INITIALIZED != status && lastAcquireWatch.elapsed(TimeUnit.HOURS) > 2) {
                        // if the stream isn't in initialized state and no writes coming in, then close the stream
                        shouldClose = true;
                        closeReason = "Stream not used anymore";
                        break;
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
                    requestClose(closeReason);
                }
            } catch (Exception ex) {
                logger.error("Stream thread {} threw unhandled exception : ", name, ex);
                setStreamInErrorStatus();
                requestClose("Unhandled exception");
            }
        }

        void scheduleTimeout(final StreamOp op) {
            final Timeout timeout = dlTimer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    if (!timeout.isCancelled()) {
                        serviceTimeout.inc();
                        handleServiceTimeout("Operation " + op.getClass().getName() + " timeout");
                    }
                }
            }, serviceTimeoutMs, TimeUnit.MILLISECONDS);
            op.responseHeader().ensure(new Function0<BoxedUnit>() {
                @Override
                public BoxedUnit apply() {
                    timeout.cancel();
                    return null;
                }
            });
        }

        void scheduleRemoval(final String streamName, long delayMs) {
            logger.info("Scheduling removal of stream {} from cache after {} sec.", name, delayMs);
            final Timeout timeout = dlTimer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    if (streams.remove(streamName, Stream.this)) {
                        logger.info("Removed cached stream {} after probation.", name);
                    } else {
                        logger.info("Cached stream {} already removed.", name);
                    }
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }

        /**
         * Close the stream and schedule cache eviction at some point in the future.
         * We delay this as a way to place the stream in a probationary state--cached
         * in the proxy but unusable.
         * This mechanism helps the cluster adapt to situations where a proxy has
         * persistent connectivity/availability issues, because it keeps an affected
         * stream off the proxy for a period of time, hopefully long enough for the
         * issues to be resolved, or for whoop to kick in and kill the shard.
         */
        synchronized void handleServiceTimeout(String reason) {
            if (StreamStatus.isUnavailable(status)) {
                return;
            }
            // Mark stream in error state
            setStreamInErrorStatus();

            // Async close request, and schedule eviction when its done.
            Future<Void> closeFuture = requestClose(reason, false /* dont remove */);
            closeFuture.onSuccess(new AbstractFunction1<Void, BoxedUnit>() {
                @Override
                public BoxedUnit apply(Void result) {
                    scheduleRemoval(name, streamProbationTimeoutMs);
                    return BoxedUnit.UNIT;
                }
            });
        }

        /**
         * Wait for stream being re-acquired if needed. Otherwise, execute the <i>op</i> immediately.
         *
         * @param op
         *          stream operation to execute.
         */
        void waitIfNeededAndWrite(StreamOp op) {
            // Let stream acquire thread know a write has been attempted.
            writeSinceLastAcquire = true;

            // Timeout stream op if requested.
            if (serviceTimeoutMs > 0) {
                scheduleTimeout(op);
            }

            op.responseHeader().addEventListener(new FutureEventListener<ResponseHeader>() {
                @Override
                public void onSuccess(ResponseHeader header) {
                    if (header.getLocation() != null || header.getCode() == StatusCode.FOUND) {
                        redirects.inc();
                    }
                    countStatusCode(header.getCode());
                }
                @Override
                public void onFailure(Throwable cause) {
                }
            });

            boolean notifyAcquireThread = false;
            boolean completeOpNow = false;
            boolean success = true;
            if (StreamStatus.isUnavailable(status)) {
                // Stream is closed, fail the op immediately
                op.fail(new StreamUnavailableException("Stream " + name + " is closed."));
                return;
            } if (StreamStatus.INITIALIZED == status && writer != null) {
                completeOpNow = true;
                success = true;
            } else {
                synchronized (this) {
                    if (StreamStatus.isUnavailable(status)) {
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
                    } else if (failFastOnStreamNotReady) {
                        notifyAcquireThread = true;
                        completeOpNow = false;
                        success = false;
                        op.fail(new StreamNotReadyException("Stream " + name + " is not ready; status = " + status));
                    } else { // closing & initializing
                        notifyAcquireThread = true;
                        pendingOps.add(op);
                        pendingOpsCounter.incrementAndGet();
                        if (1 == pendingOps.size()) {
                            if (op instanceof HeartbeatOp) {
                                ((HeartbeatOp) op).setWriteControlRecord(true);
                            }
                        }
                    }
                }
            }
            if (notifyAcquireThread) {
                notifyStreamLockWaiters();
            }
            if (completeOpNow) {
                executeOp(op, success);
            }
        }

        void notifyStreamLockWaiters() {
            synchronized (streamLock) {
                streamLock.notifyAll();
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
                if (StreamStatus.isUnavailable(status)) {
                    op.fail(new StreamUnavailableException("Stream " + name + " is closed."));
                    return;
                }
                doExecuteOp(op, success);
            } finally {
                closeLock.readLock().unlock();
            }
        }

        private void doExecuteOp(final StreamOp op, boolean success) {
            final AsyncLogWriter writer;
            final Throwable lastException;
            synchronized (this) {
                writer = this.writer;
                lastException = this.lastException;
            }
            if (null != writer && success) {
                op.execute(writer).addEventListener(new FutureEventListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        // nop
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        boolean countAsException = true;
                        if (cause instanceof DLException) {
                            final DLException dle = (DLException) cause;
                            switch (dle.getCode()) {
                            case FOUND:
                                assert(cause instanceof OwnershipAcquireFailedException);
                                countAsException = false;
                                handleOwnershipAcquireFailedException(op, (OwnershipAcquireFailedException) cause);
                                break;
                            case ALREADY_CLOSED:
                                assert(cause instanceof AlreadyClosedException);
                                op.fail(cause);
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
                            case OVER_CAPACITY:
                                op.fail(cause);
                                break;
                            // exceptions that *could* / *might* be recovered by creating a new writer
                            default:
                                handleRecoverableDLException(op, cause);
                                break;
                            }
                        } else {
                            handleUnknownException(op, cause);
                        }
                        if (countAsException) {
                            countException(cause, exceptionStatLogger);
                        }
                    }
                });
            } else {
                op.fail(lastException);
            }
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
                    oldWriter = setStreamStatus(StreamStatus.FAILED, StreamStatus.INITIALIZED,
                            null, null, cause);
                    statusChanged = true;
                }
            }
            if (statusChanged) {
                abort(oldWriter);
                logger.error("Failed to write data into stream {} : ", name, cause);
            }
            op.fail(cause);
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
                    oldWriter = setStreamStatus(StreamStatus.FAILED, StreamStatus.INITIALIZED,
                            null, null, cause);
                    statusChanged = true;
                }
            }
            if (statusChanged) {
                abort(oldWriter);
                logger.error("Failed to write data into stream {} : ", name, cause);
            }
            op.fail(cause);
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
            logger.warn("Failed to write data into stream {} because stream is acquired by {} : {}",
                        new Object[] { name, oafe.getCurrentOwner(), oafe.getMessage() });
            AsyncLogWriter oldWriter = null;
            boolean statusChanged = false;
            synchronized (this) {
                if (StreamStatus.INITIALIZED == status) {
                    oldWriter =
                        setStreamStatus(StreamStatus.BACKOFF, StreamStatus.INITIALIZED,
                                null, oafe.getCurrentOwner(), oafe);
                    statusChanged = true;
                }
            }
            if (statusChanged) {
                abort(oldWriter);
            }
            op.fail(oafe);
        }

        /**
         * Handling already closed exception.
         */
        private void handleAlreadyClosedException(AlreadyClosedException ace) {
            unexpectedExceptions.inc();
            logger.error("Encountered unexpected exception when writing data into stream {} : ", name, ace);
            triggerShutdown();
        }

        void acquireStream() {
            // Reset this flag so the acquire thread knows whether re-acquire is needed.
            writeSinceLastAcquire = false;

            Queue<StreamOp> oldPendingOps;
            boolean success;
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                AsyncLogWriter w = manager.startAsyncLogSegmentNonPartitioned();
                synchronized (this) {
                    setStreamStatus(StreamStatus.INITIALIZED,
                            StreamStatus.INITIALIZING, w, null, null);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = true;
                }
            } catch (final AlreadyClosedException ace) {
                countException(ace, exceptionStatLogger);
                handleAlreadyClosedException(ace);
                return;
            } catch (final OwnershipAcquireFailedException oafe) {
                logger.warn("Failed to acquire stream ownership for {}, current owner is {} : {}",
                            new Object[] { name, oafe.getCurrentOwner(), oafe.getMessage() });
                synchronized (this) {
                    setStreamStatus(StreamStatus.BACKOFF,
                            StreamStatus.INITIALIZING, null, oafe.getCurrentOwner(), oafe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            } catch (final IOException ioe) {
                countException(ioe, exceptionStatLogger);
                logger.error("Failed to initialize stream {} : ", name, ioe);
                synchronized (this) {
                    setStreamStatus(StreamStatus.FAILED,
                            StreamStatus.INITIALIZING, null, null, ioe);
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<StreamOp>();
                    success = false;
                }
            }
            if (success) {
                streamAcquireStat.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            } else {
                streamAcquireStat.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            }
            for (StreamOp op : oldPendingOps) {
                executeOp(op, success);
                pendingOpsCounter.decrementAndGet();
            }
        }

        synchronized void setStreamInErrorStatus() {
            if (StreamStatus.CLOSING == status || StreamStatus.CLOSED == status) {
                return;
            }
            this.status = StreamStatus.ERROR;
        }

        /**
         * Update the stream status. The changes are only applied when there isn't status changed.
         *
         * @param newStatus
         *          new status
         * @param oldStatus
         *          old status
         * @param writer
         *          new log writer
         * @param owner
         *          new owner
         * @param t
         *          new exception
         * @return old writer if it exists
         */
        synchronized AsyncLogWriter setStreamStatus(StreamStatus newStatus,
                                                    StreamStatus oldStatus,
                                                    AsyncLogWriter writer,
                                                    String owner,
                                                    Throwable t) {
            if (oldStatus != this.status) {
                logger.info("Stream {} status already changed from {} -> {} when trying to change it to {}",
                        new Object[] { name, oldStatus, this.status, newStatus });
                return null;
            }

            AsyncLogWriter oldWriter = this.writer;
            this.writer = writer;
            if (null != owner && owner.equals(clientId)) {
                unexpectedExceptions.inc();
                logger.error("I am waiting myself {} to release lock on stream {}, so have to shut myself down :",
                             new Object[] { owner, name, t });
                // I lost the ownership but left a lock over zookeeper
                // I should not ask client to redirect to myself again as I can't handle it :(
                // shutdown myself
                triggerShutdown();
                this.owner = null;
            } else {
                this.owner = owner;
            }
            this.lastException = t;
            this.status = newStatus;
            if (StreamStatus.BACKOFF == newStatus && null != owner) {
                // start failure watch
                this.lastAcquireFailureWatch.reset().start();
            }
            if (StreamStatus.INITIALIZED == newStatus) {
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

        Future<Void> requestClose(String reason) {
            return requestClose(reason, true);
        }

        Future<Void> requestClose(String reason, boolean uncache) {
            final boolean abort;
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSING == status ||
                    StreamStatus.CLOSED == status) {
                    return closePromise;
                }
                logger.info("Request to close stream {} : {}", getName(), reason);
                // if the stream isn't closed from INITIALIZED state, we abort the stream instead of closing it.
                abort = StreamStatus.INITIALIZED != status;
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
                        close(abort);
                        closePromise.setValue(null);
                    }
                });
            } catch (RejectedExecutionException ree) {
                logger.warn("Failed to schedule closing stream {}, close it directly : ", name, ree);
                close(abort);
                closePromise.setValue(null);
            }
            if (uncache) {
                closePromise.onSuccess(new AbstractFunction1<Void, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(Void result) {
                        if (streams.remove(name, Stream.this)) {
                            logger.info("Removed cached stream {} after closed.", name);
                        }
                        return BoxedUnit.UNIT;
                    }
                });
            }
            return closePromise;
        }

        private void delete() throws IOException {
            if (null != writer) {
                writer.close();
                synchronized (this) {
                    writer = null;
                    lastException = new StreamUnavailableException("Stream was deleted");
                }
            }
            if (null == manager) {
                throw new UnexpectedException("No stream " + name + " to delete");
            }
            manager.delete();
        }

        /**
         * Shouldn't call close directly. The callers should call #requestClose instead
         *
         * @param shouldAbort shall we abort the stream instead of closing
         */
        private void close(boolean shouldAbort) {
            boolean abort;
            closeLock.writeLock().lock();
            try {
                if (StreamStatus.CLOSED == status) {
                    return;
                }
                abort = shouldAbort || (StreamStatus.INITIALIZED != status && StreamStatus.CLOSING != status);
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
            logger.info("Stopped threads of stream {}.", name);
            // Close the writers to release the locks before failing the requests
            if (abort) {
                abort(writer);
            } else {
                close(writer);
            }
            close(manager);
            // Failed the pending requests.
            Queue<StreamOp> oldPendingOps;
            synchronized (this) {
                oldPendingOps = pendingOps;
                pendingOps = new ArrayDeque<StreamOp>();
            }
            StreamUnavailableException closingException =
                    new StreamUnavailableException("Stream " + name + " is closed.");
            for (StreamOp op : oldPendingOps) {
                op.fail(closingException);
                pendingOpsCounter.decrementAndGet();
            }
            logger.info("Closed stream {}.", name);
        }
    }

    private final DistributedLogConfiguration dlConfig;
    private final Optional<DynamicDistributedLogConfiguration> dlDynamicConfig;
    private final DistributedLogNamespace dlNamespace;
    private final ConcurrentHashMap<String, Stream> streams =
            new ConcurrentHashMap<String, Stream>();
    private final ConcurrentHashMap<String, Stream> acquiredStreams =
            new ConcurrentHashMap<String, Stream>();

    private final int serverRegionId;
    private boolean closed = false;
    private boolean serverAcceptNewStream = true;
    private final ReentrantReadWriteLock closeLock =
            new ReentrantReadWriteLock();
    private final CountDownLatch keepAliveLatch;
    private final Object txnLock = new Object();
    private final AtomicInteger pendingOpsCounter;

    // Features
    private final FeatureProvider featureProvider;
    private final Feature featureRegionStopAcceptNewStream;

    // Stats
    private final StatsLogger statsLogger;
    // operation stats
    private final Counter bulkWritePendingStat;
    private final Counter writePendingStat;
    private final Counter redirects;
    private final Counter unexpectedExceptions;
    private final Counter bulkWriteBytes;
    private final Counter writeBytes;
    private final Counter serviceTimeout;

    // denied operations counters
    private final Counter deniedBulkWriteCounter;
    private final Counter deniedWriteCounter;
    private final Counter deniedHeartbeatCounter;
    private final Counter deniedTruncateCounter;
    private final Counter deniedDeleteCounter;
    private final Counter deniedReleaseCounter;

    private final Counter receivedRecordCounter;

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
    private final AccessControlManager accessControlManager;
    private final long delayMs;
    private final boolean failFastOnStreamNotReady;
    private final HashedWheelTimer dlTimer;
    private long serviceTimeoutMs;
    private final long streamProbationTimeoutMs;
    private final StreamConfigProvider streamConfigProvider;

    DistributedLogServiceImpl(DistributedLogConfiguration dlConf,
                              StreamConfigProvider streamConfigProvider,
                              URI uri,
                              StatsLogger statsLogger,
                              CountDownLatch keepAliveLatch)
            throws IOException {
        // Configuration.
        this.dlConfig = dlConf;
        // by default, we generate version0 dlsn for backward compatability. the proxy server
        // would update to generate version1 dlsn after all clients updates to use latest version.
        this.dlsnVersion = dlConf.getByte("server_dlsn_version", DLSN.VERSION0);
        this.serverMode = ServerMode.valueOf(dlConf.getString("server_mode", ServerMode.DURABLE.toString()));
        this.serverRegionId = dlConf.getInt("server_region_id", DistributedLogConstants.LOCAL_REGION_ID);
        int serverPort = dlConf.getInt("server_port", 0);
        int shard = dlConf.getInt("server_shard", -1);
        int numThreads = dlConf.getInt("server_threads", Runtime.getRuntime().availableProcessors());
        this.clientId = DLSocketAddress.toLockId(DLSocketAddress.getSocketAddress(serverPort), shard);
        String allocatorPoolName = String.format("allocator_%04d_%010d", serverRegionId, shard);
        dlConf.setLedgerAllocatorPoolName(allocatorPoolName);
        this.featureProvider = AbstractFeatureProvider.getFeatureProvider("", dlConf, statsLogger.scope("features"));

        // Build the namespace
        this.dlNamespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(dlConf)
                .uri(uri)
                .statsLogger(statsLogger)
                .featureProvider(this.featureProvider)
                .clientId(clientId)
                .regionId(serverRegionId)
                .build();
        this.accessControlManager = this.dlNamespace.createAccessControlManager();

        this.keepAliveLatch = keepAliveLatch;
        this.failFastOnStreamNotReady = dlConf.getFailFastOnStreamNotReady();
        this.dlTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("DLService-timer-%d").build(),
                dlConf.getTimeoutTimerTickDurationMs(),
                TimeUnit.MILLISECONDS);
        this.serviceTimeoutMs = dlConf.getServiceTimeoutMs();
        this.streamProbationTimeoutMs = dlConf.getStreamProbationTimeoutMs();
        this.streamConfigProvider = streamConfigProvider;

        // Executor Service.
        this.executorService = Executors.newScheduledThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("DistributedLogService-Executor-%d").build());
        if (ServerMode.MEM.equals(serverMode)) {
            this.delayMs = dlConf.getLong("server_latency_delay", 0);
        } else {
            this.delayMs = 0;
        }

        // service features
        this.featureRegionStopAcceptNewStream = this.featureProvider.getFeature(
                ServerFeatureKeys.REGION_STOP_ACCEPT_NEW_STREAM.name().toLowerCase());

        // status about server health
        statsLogger.registerGauge("proxy_status", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return closed ? -1 : (featureRegionStopAcceptNewStream.isAvailable() ? 3 : (serverAcceptNewStream ? 1 : 2));
            }
        });

        // Stats on server
        this.unexpectedExceptions = statsLogger.getCounter("unexpected_exceptions");
        this.pendingOpsCounter = new AtomicInteger(0);
        statsLogger.registerGauge("pending_ops", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return pendingOpsCounter.get();
            }
        });

        this.statsLogger = statsLogger;

        // Stats on requests
        StatsLogger requestsStatsLogger = statsLogger.scope("request");
        this.bulkWritePendingStat = requestsStatsLogger.getCounter("bulkWritePending");
        this.writePendingStat = requestsStatsLogger.getCounter("writePending");
        this.redirects = requestsStatsLogger.getCounter("redirect");
        this.exceptionStatLogger = requestsStatsLogger.scope("exceptions");
        this.statusCodeStatLogger = requestsStatsLogger.scope("statuscode");
        this.bulkWriteBytes = requestsStatsLogger.scope("bulkWrite").getCounter("bytes");
        this.writeBytes = requestsStatsLogger.scope("write").getCounter("bytes");
        this.serviceTimeout = statsLogger.getCounter("serviceTimeout");

        StatsLogger recordsStatsLogger = statsLogger.scope("records");
        this.receivedRecordCounter = recordsStatsLogger.getCounter("received");

        // Stats on denied requests
        StatsLogger deniedRequestsStatsLogger = requestsStatsLogger.scope("denied");
        this.deniedBulkWriteCounter = deniedRequestsStatsLogger.getCounter("bulkWrite");
        this.deniedWriteCounter = deniedRequestsStatsLogger.getCounter("write");
        this.deniedHeartbeatCounter = deniedRequestsStatsLogger.getCounter("heartbeat");
        this.deniedTruncateCounter = deniedRequestsStatsLogger.getCounter("truncate");
        this.deniedDeleteCounter = deniedRequestsStatsLogger.getCounter("delete");
        this.deniedReleaseCounter = deniedRequestsStatsLogger.getCounter("release");

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

        this.dlDynamicConfig = getDlDynamicConfig();

        logger.info("Running distributedlog server in {} mode, delay ms {}, client id {}, allocator pool {}, perstream stat {}.",
                new Object[] { serverMode, delayMs, clientId, allocatorPoolName, enablePerStreamStat });
    }

    private Optional<DynamicDistributedLogConfiguration> getDlDynamicConfig() {
        // Global dynamic stream config is a placeholder until per-stream configs are added (PUBSUB-6994).
        DynamicConfigurationFactory dynamicConfigFactory = new DynamicConfigurationFactory(executorService,
            dlConfig.getDynamicConfigReloadIntervalSec(), TimeUnit.SECONDS,
            new ConcurrentConstConfiguration(dlConfig));
        Optional<DynamicDistributedLogConfiguration> dynConfig = Optional.<DynamicDistributedLogConfiguration>absent();
        String globalDynamicConfigPath = dlConfig.getString("server_dyn_config_path", null);
        try {
            if (null != globalDynamicConfigPath) {
                dynConfig = dynamicConfigFactory.getDynamicConfiguration(globalDynamicConfigPath);
            }
        } catch (ConfigurationException ex) {
            logger.error("Unexpected configuration exception {}", ex);
        }
        return dynConfig;
    }

    Stream newStream(String name) {
        return new Stream(name);
    }

    @VisibleForTesting
    public void setServiceTimeoutMs(int serviceTimeoutMs) {
        this.serviceTimeoutMs = serviceTimeoutMs;
    }

    @VisibleForTesting
    public DistributedLogNamespace getDistributedLogNamespace() {
        return dlNamespace;
    }

    @VisibleForTesting
    ConcurrentMap<String, Stream> getCachedStreams() {
        return this.streams;
    }

    @VisibleForTesting
    ConcurrentMap<String, Stream> getAcquiredStreams() {
        return this.acquiredStreams;
    }

    java.util.concurrent.Future<?> schedule(Runnable r, long delayMs) {
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
        return handshakeWithClientInfo(new ClientInfo());
    }

    @Override
    public Future<ServerInfo> handshakeWithClientInfo(ClientInfo clientInfo) {
        ServerInfo serverInfo = new ServerInfo();
        Map<String, String> ownerships = new HashMap<String, String>();
        for (String name : acquiredStreams.keySet()) {
            if (clientInfo.isSetStreamNameRegex() && !name.matches(clientInfo.getStreamNameRegex())) {
                continue;
            }
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
                if (featureRegionStopAcceptNewStream.isAvailable()) {
                    // accept new stream is disabled in current dc
                    throw new RegionUnavailableException("Region is unavailable right now.");
                } else if (closed || !serverAcceptNewStream) {
                    // if it is closed, we would not acquire stream again.
                    return null;
                }
                writer = newStream(stream);
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
        if (!accessControlManager.allowWrite(stream)) {
            deniedWriteCounter.inc();
            return Future.value(writeResponse(operationDeniedResponseHeader()));
        }
        receivedRecordCounter.inc();
        return doWrite(stream, data);
    }

    @Override
    public Future<BulkWriteResponse> writeBulkWithContext(final String stream, List<ByteBuffer> data, WriteContext ctx) {
        if (!accessControlManager.allowWrite(stream)) {
            deniedBulkWriteCounter.inc();
            return Future.value(bulkWriteResponse(operationDeniedResponseHeader()));
        }
        bulkWritePendingStat.inc();
        receivedRecordCounter.add(data.size());
        BulkWriteOp op = new BulkWriteOp(stream, data, statsLogger);
        doExecuteStreamOp(op);
        return op.result.ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                bulkWritePendingStat.dec();
                return null;
            }
        });
    }

    @Override
    public Future<WriteResponse> writeWithContext(final String stream, ByteBuffer data, WriteContext ctx) {
        if (!accessControlManager.allowWrite(stream)) {
            deniedWriteCounter.inc();
            return Future.value(writeResponse(operationDeniedResponseHeader()));
        }
        return doWrite(stream, data);
    }

    @Override
    public Future<WriteResponse> heartbeat(String stream, WriteContext ctx) {
        if (!accessControlManager.allowAcquire(stream)) {
            deniedHeartbeatCounter.inc();
            return Future.value(writeResponse(operationDeniedResponseHeader()));
        }
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, txnLock, dlsnVersion);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<WriteResponse> heartbeatWithOptions(String stream, WriteContext ctx, HeartbeatOptions options) {
        if (!accessControlManager.allowAcquire(stream)) {
            deniedHeartbeatCounter.inc();
            return Future.value(writeResponse(operationDeniedResponseHeader()));
        }
        HeartbeatOp op = new HeartbeatOp(stream, statsLogger, txnLock, dlsnVersion);
        if (options.isSendHeartBeatToReader()) {
            op.setWriteControlRecord(true);
        }
        doExecuteStreamOp(op);
        return op.result;
    }


    @Override
    public Future<WriteResponse> truncate(String stream, String dlsn, WriteContext ctx) {
        if (!accessControlManager.allowTruncate(stream)) {
            deniedTruncateCounter.inc();
            return Future.value(writeResponse(operationDeniedResponseHeader()));
        }
        TruncateOp op = new TruncateOp(stream, DLSN.deserialize(dlsn), statsLogger);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<WriteResponse> delete(String stream, WriteContext ctx) {
        if (!accessControlManager.allowTruncate(stream)) {
            deniedDeleteCounter.inc();
            return Future.value(writeResponse(operationDeniedResponseHeader()));
        }
        DeleteOp op = new DeleteOp(stream, statsLogger, this /* stream manager */);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<WriteResponse> release(String stream, WriteContext ctx) {
        if (!accessControlManager.allowRelease(stream)) {
            deniedReleaseCounter.inc();
            return Future.value(writeResponse(operationDeniedResponseHeader()));
        }
        ReleaseOp op = new ReleaseOp(stream, statsLogger, this /* stream manager */);
        doExecuteStreamOp(op);
        return op.result;
    }

    @Override
    public Future<Void> setAcceptNewStream(boolean enabled) {
        closeLock.writeLock().lock();
        try {
            logger.info("Set AcceptNewStream = {}", enabled);
            serverAcceptNewStream = enabled;
        } finally {
            closeLock.writeLock().unlock();
        }
        return Future.Void();
    }

    private Future<WriteResponse> doWrite(final String name, ByteBuffer data) {
        writePendingStat.inc();
        receivedRecordCounter.inc();
        WriteOp op = newWriteOp(name, data);
        doExecuteStreamOp(op);
        return op.result.ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                writePendingStat.dec();
                return null;
            }
        });
    }

    private void doExecuteStreamOp(final StreamOp op) {
        Stream stream;
        try {
            stream = getLogWriter(op.streamName());
        } catch (RegionUnavailableException rue) {
            // redirect the requests to other region
            op.fail(new RegionUnavailableException("Region " + serverRegionId + " is unavailable."));
            return;
        } catch (IOException e) {
            op.fail(e);
            return;
        }
        if (null == stream) {
            // redirect the requests when stream is unavailable.
            op.fail(new ServiceUnavailableException("Server " + clientId + " is closed."));
            return;
        }
        if (op instanceof WriteOp) {
          final OpStatsLogger latencyStat = stream.streamLogger.getOpStatsLogger("write");
          final Counter bytes = stream.streamLogger.scope("write").getCounter("bytes");
          final long size = ((WriteOp) op).getPayloadSize();
          ((WriteOp) op).result.addEventListener(new FutureEventListener<WriteResponse>() {
            @Override
            public void onSuccess(WriteResponse ignoreVal) {
              latencyStat.registerSuccessfulEvent(op.stopwatch().elapsed(TimeUnit.MICROSECONDS));
              bytes.add(size);
              writeBytes.add(size);
            }

            @Override
            public void onFailure(Throwable cause) {
              latencyStat.registerFailedEvent(op.stopwatch().elapsed(TimeUnit.MICROSECONDS));
            }
          });
        } else if (op instanceof BulkWriteOp) {
          final OpStatsLogger latencyStat = stream.streamLogger.getOpStatsLogger("bulkWrite");
          final Counter bytes = stream.streamLogger.scope("bulkWrite").getCounter("bytes");
          final long size = ((BulkWriteOp) op).getPayloadSize();
          ((BulkWriteOp) op).result.addEventListener(new FutureEventListener<BulkWriteResponse>() {
            @Override
            public void onSuccess(BulkWriteResponse ignoreVal) {
              latencyStat.registerSuccessfulEvent(op.stopwatch().elapsed(TimeUnit.MICROSECONDS));
              bytes.add(size);
              bulkWriteBytes.add(size);
            }

            @Override
            public void onFailure(Throwable cause) {
              latencyStat.registerFailedEvent(op.stopwatch().elapsed(TimeUnit.MICROSECONDS));
            }
          });
        }
        stream.waitIfNeededAndWrite(op);
    }

    Future<List<Void>> closeStreams() {
        int numAcquired = acquiredStreams.size();
        int numCached = streams.size();
        logger.info("Closing all acquired streams : acquired = {}, cached = {}.",
                    numAcquired, numCached);
        Set<Stream> streamsToClose = new HashSet<Stream>();
        streamsToClose.addAll(streams.values());
        return closeStreams(streamsToClose, Optional.<RateLimiter>absent());
    }

    private Future<List<Void>> closeStreams(Set<Stream> streamsToClose, Optional<RateLimiter> rateLimiter) {
        if (streamsToClose.isEmpty()) {
            logger.info("No streams to close.");
            List<Void> emptyList = new ArrayList<Void>();
            return Future.value(emptyList);
        }
        List<Future<Void>> futures = new ArrayList<Future<Void>>(streamsToClose.size());
        for (Stream stream : streamsToClose) {
            if (rateLimiter.isPresent()) {
                rateLimiter.get().acquire();
            }
            futures.add(stream.requestClose("Close Streams"));
        }
        return Future.collect(futures);
    }

    void shutdown() {
        try {
            closeLock.writeLock().lock();
            try {
                if (closed) {
                    return;
                }
                closed = true;
            } finally {
                closeLock.writeLock().unlock();
            }
            Future<List<Void>> closeResult = closeStreams();

            logger.info("Waiting for closing all streams ...");
            try {
                Await.result(closeResult, Duration.fromTimeUnit(5, TimeUnit.MINUTES));
                logger.info("Closed all streams.");
            } catch (InterruptedException e) {
                logger.warn("Interrupted on waiting for closing all streams : ", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("Sorry, we didn't close all streams gracefully in 5 minutes : ", e);
            }

            // shutdown the dl namespace
            logger.info("Closing distributedlog namespace ...");
            dlNamespace.close();
            logger.info("Closed distributedlog namespace .");

            // shutdown the executor after requesting closing streams.
            SchedulerUtils.shutdownScheduler(executorService, 60, TimeUnit.SECONDS);

            // Shutdown timeout timer
            dlTimer.stop();
        } catch (Exception ex) {
            logger.info("Exception while shutting down distributedlog service.");
        } finally {
            // release the keepAliveLatch in case shutdown is called from a shutdown hook.
            keepAliveLatch.countDown();
            logger.info("Finished shutting down distributedlog service.");
        }
    }

    void triggerShutdown() {
        // release the keepAliveLatch to let the main thread shutdown the whole service.
        logger.info("Releasing KeepAlive Latch to trigger shutdown ...");
        keepAliveLatch.countDown();
        logger.info("Released KeepAlive Latch. Main thread will shut the service down.");
    }
}
