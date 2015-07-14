package com.twitter.distributedlog.client.routing;

import com.twitter.finagle.stats.StatsReceiver;

class RoutingServiceProvider implements RoutingService.Builder {

    final RoutingService routingService;

    RoutingServiceProvider(RoutingService routingService) {
        this.routingService = routingService;
    }

    @Override
    public RoutingService.Builder statsReceiver(StatsReceiver statsReceiver) {
        return this;
    }

    @Override
    public RoutingService build() {
        return routingService;
    }
}
