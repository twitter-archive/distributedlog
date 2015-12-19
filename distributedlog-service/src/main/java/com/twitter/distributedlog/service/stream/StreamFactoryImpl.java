package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.service.FatalErrorHandler;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.stream.StreamOpStats;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.jboss.netty.util.HashedWheelTimer;

public class StreamFactoryImpl implements StreamFactory {
    private final String clientId;
    private final StreamOpStats streamOpStats;
    private final DistributedLogConfiguration dlConfig;
    private final FeatureProvider featureProvider;
    private final StreamConfigProvider streamConfigProvider;
    private final DistributedLogNamespace dlNamespace;
    private final ScheduledExecutorService executorService;
    private final FatalErrorHandler fatalErrorHandler;
    private final HashedWheelTimer requestTimer;

    public StreamFactoryImpl(String clientId,
        StreamOpStats streamOpStats,
        DistributedLogConfiguration dlConfig,
        FeatureProvider featureProvider,
        StreamConfigProvider streamConfigProvider,
        DistributedLogNamespace dlNamespace,
        ScheduledExecutorService executorService,
        FatalErrorHandler fatalErrorHandler,
        HashedWheelTimer requestTimer) {

        this.clientId = clientId;
        this.streamOpStats = streamOpStats;
        this.dlConfig = dlConfig;
        this.featureProvider = featureProvider;
        this.streamConfigProvider = streamConfigProvider;
        this.dlNamespace = dlNamespace;
        this.executorService = executorService;
        this.fatalErrorHandler = fatalErrorHandler;
        this.requestTimer = requestTimer;
    }

    @Override
    public Stream create(String name, StreamManager streamManager) {
        return new StreamImpl(name,
            clientId,
            streamManager,
            streamOpStats,
            dlConfig,
            featureProvider,
            streamConfigProvider,
            dlNamespace,
            executorService,
            fatalErrorHandler,
            requestTimer);
    }
}
