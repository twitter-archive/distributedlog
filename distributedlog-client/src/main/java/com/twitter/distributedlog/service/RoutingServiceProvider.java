package com.twitter.distributedlog.service;

class RoutingServiceProvider implements RoutingService.Builder {

    final RoutingService routingService;

    RoutingServiceProvider(RoutingService routingService) {
        this.routingService = routingService;
    }

    @Override
    public RoutingService build() {
        return routingService;
    }
}
