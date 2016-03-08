package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;

public interface StreamFactory {

    /**
     * Create a stream object.
     *
     * @param name stream name
     * @param streamConf stream configuration
     * @param streamManager manager of streams
     * @return stream object
     */
    Stream create(String name,
                  DynamicDistributedLogConfiguration streamConf,
                  StreamManager streamManager);
}
