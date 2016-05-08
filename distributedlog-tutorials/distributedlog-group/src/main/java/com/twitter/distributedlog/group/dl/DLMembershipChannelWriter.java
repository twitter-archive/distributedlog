package com.twitter.distributedlog.group.dl;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.group.MembershipChannelWriter;
import com.twitter.distributedlog.group.util.ThriftUtil;
import com.twitter.distributedlog.thrift.group.MembershipMessage;
import com.twitter.util.Future;
import org.apache.thrift.TException;

/**
 * Write {@link MembershipMessage}
 */
public class DLMembershipChannelWriter implements MembershipChannelWriter {

    private final AsyncLogWriter writer;

    public DLMembershipChannelWriter(AsyncLogWriter writer) {
        this.writer = writer;
    }

    @Override
    public Future<DLSN> writeMessage(MembershipMessage message) {
        try {
            return writer.write(ThriftUtil.write(System.currentTimeMillis(), message));
        } catch (TException e) {
            return Future.exception(e);
        }
    }
}
