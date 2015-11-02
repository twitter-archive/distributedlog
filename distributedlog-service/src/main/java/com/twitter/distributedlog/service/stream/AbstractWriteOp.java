package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.ProtocolUtils;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.util.Future;

import java.nio.ByteBuffer;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.OpStatsLogger;

import scala.runtime.AbstractFunction1;

public abstract class AbstractWriteOp extends AbstractStreamOp<WriteResponse> {

    protected AbstractWriteOp(String stream,
                              OpStatsLogger statsLogger,
                              Long checksum,
                              Feature checksumDisabledFeature) {
        super(stream, statsLogger, checksum, checksumDisabledFeature);
    }

    @Override
    protected void fail(ResponseHeader header) {
        setResponse(ResponseUtils.write(header));
    }

    @Override
    public Long computeChecksum() {
        return ProtocolUtils.streamOpCRC32(stream);
    }

    @Override
    public Future<ResponseHeader> responseHeader() {
        return result().map(new AbstractFunction1<WriteResponse, ResponseHeader>() {
            @Override
            public ResponseHeader apply(WriteResponse response) {
                return response.getHeader();
            }
        });
    }
}
