package com.twitter.distributedlog.group.dl;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.group.ControlChannelReader;
import com.twitter.distributedlog.group.util.ThriftUtil;
import com.twitter.distributedlog.thrift.group.ControlMessage;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.Future;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Control Channel Reader
 */
public class DLControlChannelReader extends ExceptionalFunction<List<LogRecordWithDLSN>, List<ControlMessage>>
        implements ControlChannelReader {

    private final AsyncLogReader reader;

    public DLControlChannelReader(AsyncLogReader reader) {
        this.reader = reader;
    }

    @Override
    public List<ControlMessage> applyE(List<LogRecordWithDLSN> records)
            throws Throwable {
        List<ControlMessage> messages = Lists.newArrayListWithExpectedSize(records.size());
        for (LogRecordWithDLSN record : records) {
            ControlMessage msg = new ControlMessage();
            ThriftUtil.read(msg, record);
            messages.add(msg);
        }
        return messages;
    }

    @Override
    public Future<List<ControlMessage>> readMessages() {
        return reader.readBulk(100, 1, TimeUnit.SECONDS).map(this);
    }
}
