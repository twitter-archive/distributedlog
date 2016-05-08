package com.twitter.distributedlog.group;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.thrift.group.ControlMessage;
import com.twitter.util.Future;

/**
 * Write {@link com.twitter.distributedlog.thrift.group.ControlMessage}
 * to control channel.
 */
public interface ControlChannelWriter {

    /**
     * Write control message to control channel.
     *
     * @param message control message
     * @return result represents the write result.
     */
    Future<DLSN> writeMessage(ControlMessage message);
}
