package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.BKUnPartitionedAsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.StatsLogger;

import scala.runtime.AbstractFunction1;

import static com.google.common.base.Charsets.UTF_8;

public class HeartbeatOp extends AbstractWriteOp {

    static final byte[] HEARTBEAT_DATA = "heartbeat".getBytes(UTF_8);

    private boolean writeControlRecord = false;
    private Object txnLock;
    private byte dlsnVersion;

    public HeartbeatOp(String stream, StatsLogger statsLogger, Object txnLock, byte dlsnVersion) {
        super(stream, requestStat(statsLogger, "heartbeat"));
        this.txnLock = txnLock;
        this.dlsnVersion = dlsnVersion;
    }

    public HeartbeatOp setWriteControlRecord(boolean writeControlRecord) {
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
                    return ResponseUtils.writeSuccess().setDlsn(value.serialize(dlsnVersion));
                }
            });
        } else {
            return Future.value(ResponseUtils.writeSuccess());
        }
    }
}