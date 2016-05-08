package com.twitter.distributedlog.group;

import com.twitter.distributedlog.thrift.group.MembershipMessage;
import com.twitter.util.Future;

import java.util.List;

/**
 * Read messages from membership channel
 */
public interface MembershipChannelReader {

    /**
     * Read next message from control channel.
     *
     * @return result represents the received control message.
     */
    Future<List<MembershipMessage>> readMessages();
}
