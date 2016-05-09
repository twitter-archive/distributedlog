/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.service.FatalErrorHandler;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import com.twitter.distributedlog.util.OrderedScheduler;
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
    private final OrderedScheduler scheduler;
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
        OrderedScheduler scheduler,
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
        this.scheduler = scheduler;
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
            scheduler,
            fatalErrorHandler,
            requestTimer);
    }
}
