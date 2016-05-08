package com.twitter.distributedlog.group;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.thrift.group.MembershipMessage;
import com.twitter.util.Future;

/**
 * Write {@link com.twitter.distributedlog.thrift.group.MembershipMessage}
 * to membership channel
 */
public interface MembershipChannelWriter {

    /**
     * Write <i>message</i> to membership channel.
     *
     * @param message membership message
     * @return future represents the write result.
     */
    Future<DLSN> writeMessage(MembershipMessage message);

}
