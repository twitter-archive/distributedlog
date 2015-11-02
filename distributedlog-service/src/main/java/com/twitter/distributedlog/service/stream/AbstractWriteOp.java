package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.ProtocolUtils;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.util.Future;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.bookkeeper.stats.OpStatsLogger;

import scala.runtime.AbstractFunction1;

public abstract class AbstractWriteOp extends AbstractStreamOp<WriteResponse> {

    protected AbstractWriteOp(String stream,
                              OpStatsLogger statsLogger,
                              Long checksum,
                              ThreadLocal<CRC32> requestCRC) {
        super(stream, statsLogger, checksum, requestCRC);
    }

    @Override
    protected void fail(ResponseHeader header) {
        setResponse(ResponseUtils.write(header));
    }

    @Override
    public Long computeChecksum() {
        return ProtocolUtils.streamOpCRC32(requestCRC.get(), stream);
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
