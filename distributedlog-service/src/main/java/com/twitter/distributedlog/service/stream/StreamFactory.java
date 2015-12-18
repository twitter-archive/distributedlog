package com.twitter.distributedlog.service.stream;

/**
 * Create a Stream object.
 */
public interface StreamFactory {

    /**
     * Create a new Stream object.
     */
    Stream newStream(String streamName);
}