package com.twitter.distributedlog.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecordSet;
import com.twitter.distributedlog.LogRecordSetBuffer;
import com.twitter.distributedlog.client.speculative.DefaultSpeculativeRequestExecutionPolicy;
import com.twitter.distributedlog.client.speculative.SpeculativeRequestExecutionPolicy;
import com.twitter.distributedlog.client.speculative.SpeculativeRequestExecutor;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.finagle.IndividualRequestTimeoutException;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.twitter.distributedlog.LogRecord.*;

/**
 * Write to multiple streams
 */
public class DistributedLogMultiStreamWriter implements Runnable {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private DistributedLogClient _client = null;
        private List<String> _streams = null;
        private int _bufferSize = 16 * 1024; // 16k
        private int _flushIntervalMs = 10; // 10ms
        private CompressionCodec.Type _codec = CompressionCodec.Type.NONE;
        private ScheduledExecutorService _executorService = null;
        private long _requestTimeoutMs = 500; // 500ms
        private int _firstSpeculativeTimeoutMs = 50; // 50ms
        private int _maxSpeculativeTimeoutMs = 200; // 200ms
        private float _speculativeBackoffMultiplier = 2;
        private Ticker _ticker = Ticker.systemTicker();

        private Builder() {}

        /**
         * Set the distributedlog client used for multi stream writer.
         *
         * @param client
         *          distributedlog client
         * @return builder
         */
        public Builder client(DistributedLogClient client) {
            this._client = client;
            return this;
        }

        /**
         * Set the list of streams to write to.
         *
         * @param streams
         *          list of streams to write
         * @return builder
         */
        public Builder streams(List<String> streams) {
            this._streams = streams;
            return this;
        }

        /**
         * Set the output buffer size.
         * <p>If output buffer size is 0, the writes will be transmitted to
         * wire immediately.
         *
         * @param bufferSize
         *          output buffer size
         * @return builder
         */
        public Builder bufferSize(int bufferSize) {
            this._bufferSize = bufferSize;
            return this;
        }

        /**
         * Set the flush interval in milliseconds.
         *
         * @param flushIntervalMs
         *          flush interval in milliseconds.
         * @return builder
         */
        public Builder flushIntervalMs(int flushIntervalMs) {
            this._flushIntervalMs = flushIntervalMs;
            return this;
        }

        /**
         * Set compression codec.
         *
         * @param codec compression codec.
         * @return builder
         */
        public Builder compressionCodec(CompressionCodec.Type codec) {
            this._codec = codec;
            return this;
        }

        /**
         * Set the scheduler to flush output buffers.
         *
         * @param executorService
         *          executor service to flush output buffers.
         * @return builder
         */
        public Builder scheduler(ScheduledExecutorService executorService) {
            this._executorService = executorService;
            return this;
        }

        /**
         * Set request timeout in milliseconds.
         *
         * @param requestTimeoutMs
         *          request timeout in milliseconds.
         * @return builder
         */
        public Builder requestTimeoutMs(long requestTimeoutMs) {
            this._requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        /**
         * Set the first speculative timeout in milliseconds.
         * <p>The multi-streams writer does speculative writes on streams.
         * The write issues first write request to a stream, if the write request
         * doesn't respond within speculative timeout. it issues next write request
         * to a different stream. It does such speculative retries until receive
         * a success or request timeout ({@link #requestTimeoutMs(long)}).
         * <p>This setting is to configure the first speculative timeout, in milliseconds.
         *
         * @param timeoutMs
         *          timeout in milliseconds
         * @return builder
         */
        public Builder firstSpeculativeTimeoutMs(int timeoutMs) {
            this._firstSpeculativeTimeoutMs = timeoutMs;
            return this;
        }

        /**
         * Set the max speculative timeout in milliseconds.
         * <p>The multi-streams writer does speculative writes on streams.
         * The write issues first write request to a stream, if the write request
         * doesn't respond within speculative timeout. it issues next write request
         * to a different stream. It does such speculative retries until receive
         * a success or request timeout ({@link #requestTimeoutMs(long)}).
         * <p>This setting is to configure the max speculative timeout, in milliseconds.
         *
         * @param timeoutMs
         *          timeout in milliseconds
         * @return builder
         */
        public Builder maxSpeculativeTimeoutMs(int timeoutMs) {
            this._maxSpeculativeTimeoutMs = timeoutMs;
            return this;
        }

        /**
         * Set the speculative timeout backoff multiplier.
         * <p>The multi-streams writer does speculative writes on streams.
         * The write issues first write request to a stream, if the write request
         * doesn't respond within speculative timeout. it issues next write request
         * to a different stream. It does such speculative retries until receive
         * a success or request timeout ({@link #requestTimeoutMs(long)}).
         * <p>This setting is to configure the speculative timeout backoff multiplier.
         *
         * @param multiplier
         *          backoff multiplier
         * @return builder
         */
        public Builder speculativeBackoffMultiplier(float multiplier) {
            this._speculativeBackoffMultiplier = multiplier;
            return this;
        }

        /**
         * Ticker for timing.
         *
         * @param ticker
         *          ticker
         * @return builder
         * @see Ticker
         */
        public Builder clockTicker(Ticker ticker) {
            this._ticker = ticker;
            return this;
        }

        /**
         * Build the multi stream writer.
         *
         * @return the multi stream writer.
         */
        public DistributedLogMultiStreamWriter build() {
            Preconditions.checkArgument((null != _streams && !_streams.isEmpty()),
                    "No streams provided");
            Preconditions.checkNotNull(_client,
                    "No distributedlog client provided");
            Preconditions.checkNotNull(_codec,
                    "No compression codec provided");
            Preconditions.checkArgument(_firstSpeculativeTimeoutMs > 0
                    && _firstSpeculativeTimeoutMs <= _maxSpeculativeTimeoutMs
                    && _speculativeBackoffMultiplier > 0
                    && _maxSpeculativeTimeoutMs < _requestTimeoutMs,
                    "Invalid speculative timeout settings");
            return new DistributedLogMultiStreamWriter(
                    _streams,
                    _client,
                    Math.min(_bufferSize, MAX_LOGRECORDSET_SIZE),
                    _flushIntervalMs,
                    _requestTimeoutMs,
                    _firstSpeculativeTimeoutMs,
                    _maxSpeculativeTimeoutMs,
                    _speculativeBackoffMultiplier,
                    _codec,
                    _ticker,
                    _executorService);
        }
    }

    /**
     * Pending Write Request.
     */
    class PendingWriteRequest implements FutureEventListener<DLSN>,
            SpeculativeRequestExecutor {

        private final LogRecordSetBuffer recordSet;
        private AtomicBoolean complete = new AtomicBoolean(false);
        private final Stopwatch stopwatch = Stopwatch.createStarted(clockTicker);
        private int nextStream;
        private int numTriedStreams = 0;

        PendingWriteRequest(LogRecordSetBuffer recordSet) {
            this.recordSet = recordSet;
            this.nextStream = Math.abs(nextStreamId.incrementAndGet()) % numStreams;
        }

        synchronized String sendNextWrite() {
            long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (elapsedMs > requestTimeoutMs || numTriedStreams >= numStreams) {
                fail(new IndividualRequestTimeoutException(Duration.fromMilliseconds(elapsedMs)));
                return null;
            }
            try {
                return sendWriteToStream(nextStream);
            } finally {
                nextStream = (nextStream + 1) % numStreams;
                ++numTriedStreams;
            }
        }

        synchronized String sendWriteToStream(int streamId) {
            String stream = getStream(streamId);
            client.writeRecordSet(stream, recordSet)
                    .addEventListener(this);
            return stream;
        }

        @Override
        public void onSuccess(DLSN dlsn) {
            if (!complete.compareAndSet(false, true)) {
                return;
            }
            recordSet.completeTransmit(
                    dlsn.getLogSegmentSequenceNo(),
                    dlsn.getEntryId(),
                    dlsn.getSlotId());
        }

        @Override
        public void onFailure(Throwable cause) {
            sendNextWrite();
        }

        private void fail(Throwable cause) {
            if (!complete.compareAndSet(false, true)) {
                return;
            }
            recordSet.abortTransmit(cause);
        }

        @Override
        public Future<Boolean> issueSpeculativeRequest() {
            return Future.value(!complete.get() && null != sendNextWrite());
        }
    }

    private final int numStreams;
    private final List<String> streams;
    private final DistributedLogClient client;
    private final int bufferSize;
    private final long requestTimeoutMs;
    private final SpeculativeRequestExecutionPolicy speculativePolicy;
    private final Ticker clockTicker;
    private final CompressionCodec.Type codec;
    private final ScheduledExecutorService scheduler;
    private final boolean ownScheduler;
    private final AtomicInteger nextStreamId;
    private LogRecordSet.Writer recordSetWriter;

    private DistributedLogMultiStreamWriter(List<String> streams,
                                            DistributedLogClient client,
                                            int bufferSize,
                                            int flushIntervalMs,
                                            long requestTimeoutMs,
                                            int firstSpecultiveTimeoutMs,
                                            int maxSpeculativeTimeoutMs,
                                            float speculativeBackoffMultiplier,
                                            CompressionCodec.Type codec,
                                            Ticker clockTicker,
                                            ScheduledExecutorService scheduler) {
        this.streams = Lists.newArrayList(streams);
        this.numStreams = this.streams.size();
        this.client = client;
        this.bufferSize = bufferSize;
        this.requestTimeoutMs = requestTimeoutMs;
        this.codec = codec;
        this.clockTicker = clockTicker;
        if (null == scheduler) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("MultiStreamWriterFlushThread-%d")
                            .build());
            this.ownScheduler = true;
        } else {
            this.scheduler = scheduler;
            this.ownScheduler = false;
        }
        this.speculativePolicy = new DefaultSpeculativeRequestExecutionPolicy(
                firstSpecultiveTimeoutMs,
                maxSpeculativeTimeoutMs,
                speculativeBackoffMultiplier);
        // shuffle the streams
        Collections.shuffle(this.streams);
        this.nextStreamId = new AtomicInteger(0);
        this.recordSetWriter = newRecordSetWriter();

        if (flushIntervalMs > 0) {
            this.scheduler.scheduleAtFixedRate(
                    this,
                    flushIntervalMs,
                    flushIntervalMs,
                    TimeUnit.MILLISECONDS);
        }
    }

    String getStream(int streamId) {
        return streams.get(streamId);
    }

    LogRecordSet.Writer getLogRecordSetWriter() {
        return recordSetWriter;
    }

    private LogRecordSet.Writer newRecordSetWriter() {
        return LogRecordSet.newWriter(
                bufferSize,
                codec);
    }

    public synchronized Future<DLSN> write(ByteBuffer buffer) {
        int logRecordSize = buffer.remaining();
        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            return Future.exception(new LogRecordTooLongException(
                    "Log record of size " + logRecordSize + " written when only "
                            + MAX_LOGRECORD_SIZE + " is allowed"));
        }
        // if exceed max number of bytes
        if ((recordSetWriter.getNumBytes() + logRecordSize) > MAX_LOGRECORDSET_SIZE) {
            flush();
        }
        Promise<DLSN> writePromise = new Promise<DLSN>();
        try {
            recordSetWriter.writeRecord(buffer, writePromise);
        } catch (LogRecordTooLongException e) {
            return Future.exception(e);
        } catch (WriteException e) {
            recordSetWriter.abortTransmit(e);
            recordSetWriter = newRecordSetWriter();
            return Future.exception(e);
        }
        if (recordSetWriter.getNumBytes() >= bufferSize) {
            flush();
        }
        return writePromise;
    }

    @Override
    public void run() {
        flush();
    }

    private void flush() {
        LogRecordSet.Writer recordSetToFlush;
        synchronized (this) {
            if (recordSetWriter.getNumRecords() == 0) {
                return;
            }
            recordSetToFlush = recordSetWriter;
            recordSetWriter = newRecordSetWriter();
        }
        transmit(recordSetToFlush);
    }

    private void transmit(LogRecordSet.Writer recordSetToFlush) {
        PendingWriteRequest writeRequest =
                new PendingWriteRequest(recordSetToFlush);
        this.speculativePolicy.initiateSpeculativeRequest(scheduler, writeRequest);
    }

    public void close() {
        if (ownScheduler) {
            this.scheduler.shutdown();
        }
    }

}
