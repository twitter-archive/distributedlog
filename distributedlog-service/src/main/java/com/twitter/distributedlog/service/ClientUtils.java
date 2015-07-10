package com.twitter.distributedlog.service;

import com.twitter.distributedlog.client.DistributedLogClientImpl;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import org.apache.commons.lang3.tuple.Pair;

public class ClientUtils {

    public static Pair<DistributedLogClient, MonitorServiceClient> buildClient(DistributedLogClientBuilder builder) {
        DistributedLogClientImpl clientImpl = builder.buildClient();
        return Pair.of((DistributedLogClient) clientImpl, (MonitorServiceClient) clientImpl);
    }
}
