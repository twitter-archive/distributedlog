package com.twitter.distributedlog.group;

import com.twitter.distributedlog.thrift.group.ControlMessage;
import com.twitter.util.Future;

import java.util.List;

/**
 * Read messages from control channel
 */
public interface ControlChannelReader {

    /**
     * Read next message from control channel.
     *
     * @return result represents the received control message.
     */
    Future<List<ControlMessage>> readMessages();

}
