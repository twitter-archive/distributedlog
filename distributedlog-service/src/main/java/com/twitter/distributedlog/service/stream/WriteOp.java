package com.twitter.distributedlog.service.stream;

import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Try;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
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
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.runtime.AbstractFunction1;

public class WriteOp extends AbstractWriteOp implements WriteOpWithPayload {
    static final Logger logger = LoggerFactory.getLogger(WriteOp.class);

    final byte[] payload;

    // Record stats
    private final Counter successRecordCounter;
    private final Counter failureRecordCounter;
    private final Counter redirectRecordCounter;

    private final Object txnLock;
    private final boolean serverModeDurable;
    private final ScheduledExecutorService executorService;
    private final byte dlsnVersion;
    private final long delayMs;

    public WriteOp(String stream, ByteBuffer data, StatsLogger statsLogger,
            Object txnLock, boolean serverModeDurable, ScheduledExecutorService executorService,
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
        this.serverModeDurable = serverModeDurable;
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
        if (serverModeDurable) {
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