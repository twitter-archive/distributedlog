package com.twitter.distributedlog.service.stream;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.twitter.util.ConstFuture;
import com.twitter.util.Future$;
import com.twitter.util.Future;
import com.twitter.util.Try;

import com.twitter.distributedlog.AlreadyClosedException;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteResponse;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

import scala.runtime.AbstractFunction1;

public class BulkWriteOp extends AbstractStreamOp<BulkWriteResponse> implements WriteOpWithPayload {
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

    public BulkWriteOp(String stream, List<ByteBuffer> buffers, StatsLogger statsLogger) {
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
                        ResponseUtils.bulkWriteSuccess().setWriteResponses(writeResponses);

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
                            WriteResponse writeResponse = ResponseUtils.writeSuccess().setDlsn(dlsn.serialize());
                            writeResponses.add(writeResponse);
                            successRecordCounter.inc();
                        } catch (Exception ioe) {
                            WriteResponse writeResponse = ResponseUtils.write(ResponseUtils.exceptionToHeader(ioe));
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
        result.setValue(ResponseUtils.bulkWrite(header));
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