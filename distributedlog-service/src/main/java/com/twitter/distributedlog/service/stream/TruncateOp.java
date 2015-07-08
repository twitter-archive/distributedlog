package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.runtime.AbstractFunction1;

public class TruncateOp extends AbstractWriteOp {

    static final Logger logger = LoggerFactory.getLogger(TruncateOp.class);

    final DLSN dlsn;

    public TruncateOp(String stream, DLSN dlsn, StatsLogger statsLogger) {
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
                return ResponseUtils.writeSuccess();
            }
        });
    }
}
