package com.twitter.distributedlog.group.dl;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.group.ControlChannelWriter;
import com.twitter.distributedlog.group.util.ThriftUtil;
import com.twitter.distributedlog.thrift.group.ControlMessage;
import com.twitter.util.Future;
import org.apache.thrift.TException;

/**
 * Write {@link ControlMessage}
 */
public class DLControlChannelWriter implements ControlChannelWriter {

    private final AsyncLogWriter writer;

    public DLControlChannelWriter(AsyncLogWriter writer) {
        this.writer = writer;
    }

    @Override
    public Future<DLSN> writeMessage(ControlMessage message) {
        try {
            return writer.write(ThriftUtil.write(System.currentTimeMillis(), message));
        } catch (TException e) {
            return Future.exception(e);
        }
    }
}
