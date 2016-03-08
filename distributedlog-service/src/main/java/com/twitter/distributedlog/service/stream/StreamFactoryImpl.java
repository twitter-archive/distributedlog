package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.service.FatalErrorHandler;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import java.util.concurrent.ScheduledExecutorService;

import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.jboss.netty.util.HashedWheelTimer;

public class StreamFactoryImpl implements StreamFactory {
    private final String clientId;
    private final StreamOpStats streamOpStats;
    private final ServerConfiguration serverConfig;
    private final DistributedLogConfiguration dlConfig;
    private final FeatureProvider featureProvider;
    private final StreamConfigProvider streamConfigProvider;
    private final StreamPartitionConverter streamPartitionConverter;
    private final DistributedLogNamespace dlNamespace;
    private final ScheduledExecutorService executorService;
    private final FatalErrorHandler fatalErrorHandler;
    private final HashedWheelTimer requestTimer;

    public StreamFactoryImpl(String clientId,
        StreamOpStats streamOpStats,
        ServerConfiguration serverConfig,
        DistributedLogConfiguration dlConfig,
        FeatureProvider featureProvider,
        StreamConfigProvider streamConfigProvider,
        StreamPartitionConverter streamPartitionConverter,
        DistributedLogNamespace dlNamespace,
        ScheduledExecutorService executorService,
        FatalErrorHandler fatalErrorHandler,
        HashedWheelTimer requestTimer) {

        this.clientId = clientId;
        this.streamOpStats = streamOpStats;
        this.serverConfig = serverConfig;
        this.dlConfig = dlConfig;
        this.featureProvider = featureProvider;
        this.streamConfigProvider = streamConfigProvider;
        this.streamPartitionConverter = streamPartitionConverter;
        this.dlNamespace = dlNamespace;
        this.executorService = executorService;
        this.fatalErrorHandler = fatalErrorHandler;
        this.requestTimer = requestTimer;
    }

    @Override
    public Stream create(String name,
                         DynamicDistributedLogConfiguration streamConf,
                         StreamManager streamManager) {
        return new StreamImpl(name,
            streamPartitionConverter.convert(name),
            clientId,
            streamManager,
            streamOpStats,
            serverConfig,
            dlConfig,
            streamConf,
            featureProvider,
            streamConfigProvider,
            dlNamespace,
            executorService,
            fatalErrorHandler,
            requestTimer);
    }
}
