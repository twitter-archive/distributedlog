package com.twitter.distributedlog.group.dl;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.group.MembershipChannelReader;
import com.twitter.distributedlog.group.util.ThriftUtil;
import com.twitter.distributedlog.thrift.group.MembershipMessage;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.Future;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DLMembershipChannelReader extends ExceptionalFunction<List<LogRecordWithDLSN>, List<MembershipMessage>>
        implements MembershipChannelReader {

    private final AsyncLogReader reader;

    public DLMembershipChannelReader(AsyncLogReader reader) {
        this.reader = reader;
    }

    @Override
    public Future<List<MembershipMessage>> readMessages() {
        return reader.readBulk(100, 1, TimeUnit.SECONDS).map(this);
    }

    @Override
    public List<MembershipMessage> applyE(List<LogRecordWithDLSN> records)
            throws Throwable {
        List<MembershipMessage> messages = Lists.newArrayListWithExpectedSize(records.size());
        for (LogRecordWithDLSN record : records) {
            MembershipMessage msg = new MembershipMessage();
            ThriftUtil.read(msg, record);
            messages.add(msg);
        }
        return messages;
    }
}
