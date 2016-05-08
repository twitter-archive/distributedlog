package com.twitter.distributedlog.group.util;

import com.twitter.distributedlog.LogRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class ThriftUtil {

    public static final TProtocolFactory PROTO = new TBinaryProtocol.Factory();

    public static <T extends TBase, F extends TFieldIdEnum> LogRecord write(
            long transactionId, TBase<T, F> message) throws TException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        message.write(ThriftUtil.PROTO.getProtocol(new TIOStreamTransport(baos)));
        return new LogRecord(transactionId, baos.toByteArray());
    }

    public static <T extends TBase, F extends TFieldIdEnum> void read(
            TBase<T, F> message, LogRecord record) throws TException {
        message.read(ThriftUtil.PROTO.getProtocol(
                new TIOStreamTransport(new ByteArrayInputStream(record.getPayload()))));
    }

}
