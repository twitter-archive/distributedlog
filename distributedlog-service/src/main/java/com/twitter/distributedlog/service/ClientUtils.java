package com.twitter.distributedlog.service;

import org.apache.commons.lang3.tuple.Pair;

public class ClientUtils {

    public static Pair<DistributedLogClient, MonitorServiceClient> buildClient(DistributedLogClientBuilder builder) {
        DistributedLogClientBuilder.DistributedLogClientImpl clientImpl = builder.buildClient();
        return Pair.of((DistributedLogClient) clientImpl, (MonitorServiceClient) clientImpl);
    }
}
