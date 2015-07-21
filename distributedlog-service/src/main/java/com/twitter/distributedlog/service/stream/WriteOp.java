package com.twitter.distributedlog.service.stream;

import com.twitter.util.FutureEventListener;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Try;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.service.DistributedLogServiceImpl;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.runtime.AbstractFunction1;

public class WriteOp extends AbstractWriteOp implements WriteOpWithPayload {
    static final Logger logger = LoggerFactory.getLogger(WriteOp.class);

    final byte[] payload;

    // Stats
    private final Counter successRecordCounter;
    private final Counter failureRecordCounter;
    private final Counter redirectRecordCounter;
    private final OpStatsLogger latencyStat;
    private final Counter bytes;
    private final Counter writeBytes;

    private final Object txnLock;
    private final ScheduledExecutorService executorService;
    private final byte dlsnVersion;
    private final long delayMs;
    private final boolean durableServerMode;

    public WriteOp(String stream, ByteBuffer data, StatsLogger statsLogger,
            Object txnLock, ScheduledExecutorService executorService,
            DistributedLogConfiguration conf) {
        super(stream, requestStat(statsLogger, "write"));
        payload = new byte[data.remaining()];
        data.get(payload);

        StreamOpStats streamOpStats = new StreamOpStats(statsLogger, conf);
        this.successRecordCounter = streamOpStats.recordsCounter("success");
        this.failureRecordCounter = streamOpStats.recordsCounter("failure");
        this.redirectRecordCounter = streamOpStats.recordsCounter("redirect");
        this.writeBytes = streamOpStats.scopedRequestCounter("write", "bytes");
        this.latencyStat = streamOpStats.streamRequestLatencyStat(stream, "write");
        this.bytes = streamOpStats.streamRequestCounter(stream, "write", "bytes");

        this.txnLock = txnLock;
        this.executorService = executorService;
        this.dlsnVersion = conf.getByte("server_dlsn_version", DLSN.VERSION0);
        this.delayMs = conf.getLong("server_latency_delay", 0);
        this.durableServerMode = durableServerMode(conf);
    }

    private boolean durableServerMode(DistributedLogConfiguration conf) {
        String memServerModeString = DistributedLogServiceImpl.ServerMode.MEM.toString();
        return !memServerModeString.equals(conf.getString("server_mode"));
    }

    @Override
    public long getPayloadSize() {
      return payload.length;
    }

    @Override
    public void preExecute() {
      final long size = getPayloadSize();
      result().addEventListener(new FutureEventListener<WriteResponse>() {
        @Override
        public void onSuccess(WriteResponse ignoreVal) {
          latencyStat.registerSuccessfulEvent(stopwatch().elapsed(TimeUnit.MICROSECONDS));
          bytes.add(size);
          writeBytes.add(size);
        }
        @Override
        public void onFailure(Throwable cause) {
          latencyStat.registerFailedEvent(stopwatch().elapsed(TimeUnit.MICROSECONDS));
        }
      });
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
        if (durableServerMode) {
            return writeResult.map(new AbstractFunction1<DLSN, WriteResponse>() {
                @Override
                public WriteResponse apply(DLSN value) {
                    successRecordCounter.inc();
                    return ResponseUtils.writeSuccess().setDlsn(value.serialize(dlsnVersion));
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
                            writePromise.setValue(ResponseUtils.writeSuccess().setDlsn(Long.toString(txnIdToReturn)));
                            successRecordCounter.inc();
                        }
                    }, delayMs, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException ree) {
                    opStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                    writePromise.setValue(ResponseUtils.writeSuccess().setDlsn(Long.toString(txnIdToReturn)));
                    successRecordCounter.inc();
                }
            } else {
                opStatsLogger.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
                writePromise.setValue(ResponseUtils.writeSuccess().setDlsn(Long.toString(txnId)));
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