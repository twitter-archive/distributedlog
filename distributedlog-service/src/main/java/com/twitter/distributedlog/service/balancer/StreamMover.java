package com.twitter.distributedlog.service.balancer;

public interface StreamMover {

    /**
     * Move given stream <i>streamName</i>
     *
     * @param streamName
     *          stream name to move
     * @return <i>true</i> if successfully moved the stream, <i>false</i> when failure happens.
     * @throws Exception
     */
    boolean moveStream(final String streamName);
}
