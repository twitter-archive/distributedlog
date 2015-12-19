package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.service.FatalErrorHandler;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.stream.StreamOpStats;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.feature.FeatureProvider;

public interface StreamFactory {

    /**
     * Create a stream object.
     * @param last known owner for the stream
     */
    Stream create(String name, StreamManager streamManager);
}
