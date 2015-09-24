package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.Future;

import java.util.zip.CRC32;

import org.apache.bookkeeper.stats.StatsLogger;

import scala.runtime.AbstractFunction1;

public class ReleaseOp extends AbstractWriteOp {
    private final StreamManager streamManager;

    public ReleaseOp(String stream,
                     StatsLogger statsLogger,
                     StreamManager streamManager,
                     Long checksum,
                     ThreadLocal<CRC32> requestCRC) {
        super(stream, requestStat(statsLogger, "release"), checksum, requestCRC);
        this.streamManager = streamManager;
    }

    @Override
    protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                              Sequencer sequencer,
                                              Object txnLock) {
        Future<Void> result = streamManager.closeAndRemoveAsync(streamName());
        return result.map(new AbstractFunction1<Void, WriteResponse>() {
            @Override
            public WriteResponse apply(Void value) {
                return ResponseUtils.writeSuccess();
            }
        });
    }
}
