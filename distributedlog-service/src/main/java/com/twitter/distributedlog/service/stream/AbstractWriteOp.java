package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.OpStatsLogger;

import scala.runtime.AbstractFunction1;

public abstract class AbstractWriteOp extends AbstractStreamOp<WriteResponse> {

    protected AbstractWriteOp(String stream, OpStatsLogger statsLogger) {
        super(stream, statsLogger);
    }

    @Override
    protected void fail(ResponseHeader header) {
        result.setValue(ResponseUtils.write(header));
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
