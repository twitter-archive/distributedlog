package com.twitter.distributedlog.client.routing;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.twitter.distributedlog.client.resolver.RegionResolver;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Set;

/**
 * Chain multiple routing services
 */
public class RegionsRoutingService implements RoutingService {

    private static final Logger logger = LoggerFactory.getLogger(RegionsRoutingService.class);

    @Deprecated
    public static RegionsRoutingService of(RegionResolver regionResolver,
                                         RoutingService...services) {
        return new RegionsRoutingService(regionResolver, services);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements RoutingService.Builder {

        private RegionResolver _resolver;
        private RoutingService.Builder[] _routingServiceBuilders;
        private StatsReceiver _statsReceiver = NullStatsReceiver.get();

        private Builder() {}

        public Builder routingServiceBuilders(RoutingService.Builder...builders) {
            this._routingServiceBuilders = builders;
            return this;
        }

        public Builder resolver(RegionResolver regionResolver) {
            this._resolver = regionResolver;
            return this;
        }

        @Override
        public RoutingService.Builder statsReceiver(StatsReceiver statsReceiver) {
            this._statsReceiver = statsReceiver;
            return this;
        }

        @Override
        public RegionsRoutingService build() {
            Preconditions.checkNotNull(_routingServiceBuilders, "No routing service builder provided.");
            Preconditions.checkNotNull(_resolver, "No region resolver provided.");
            Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided");
            RoutingService[] services = new RoutingService[_routingServiceBuilders.length];
            for (int i = 0; i < services.length; i++) {
                String statsScope;
                if (0 == i) {
                    statsScope = "local";
                } else {
                    statsScope = "remote_" + i;
                }
                services[i] = _routingServiceBuilders[i]
                        .statsReceiver(_statsReceiver.scope(statsScope))
                        .build();
            }
            return new RegionsRoutingService(_resolver, services);
        }
    }

    protected final RegionResolver regionResolver;
    protected final RoutingService[] routingServices;

    private RegionsRoutingService(RegionResolver resolver,
                                  RoutingService[] routingServices) {
        this.regionResolver = resolver;
        this.routingServices = routingServices;
    }

    @Override
    public Set<SocketAddress> getHosts() {
        Set<SocketAddress> hosts = Sets.newHashSet();
        for (RoutingService rs : routingServices) {
            hosts.addAll(rs.getHosts());
        }
        return hosts;
    }

    @Override
    public void startService() {
        for (RoutingService service : routingServices) {
            service.startService();
        }
        logger.info("Regions Routing Service Started");
    }

    @Override
    public void stopService() {
        for (RoutingService service : routingServices) {
            service.stopService();
        }
        logger.info("Regions Routing Service Stopped");
    }

    @Override
    public RoutingService registerListener(RoutingListener listener) {
        for (RoutingService service : routingServices) {
            service.registerListener(listener);
        }
        return this;
    }

    @Override
    public RoutingService unregisterListener(RoutingListener listener) {
        for (RoutingService service : routingServices) {
            service.registerListener(listener);
        }
        return this;
    }

    @Override
    public SocketAddress getHost(String key, RoutingContext routingContext)
            throws NoBrokersAvailableException {
        for (RoutingService service : routingServices) {
            try {
                SocketAddress addr = service.getHost(key, routingContext);
                if (routingContext.hasUnavailableRegions()) {
                    // current region is unavailable
                    String region = regionResolver.resolveRegion(addr);
                    if (routingContext.isUnavailableRegion(region)) {
                        continue;
                    }
                }
                if (!routingContext.isTriedHost(addr)) {
                    return addr;
                }
            } catch (NoBrokersAvailableException nbae) {
                // if there isn't broker available in current service, try next service.
                logger.debug("No brokers available in region {} : ", service, nbae);
            }
        }
        throw new NoBrokersAvailableException("No host found for " + key + ", routing context : " + routingContext);
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        for (RoutingService service : routingServices) {
            service.removeHost(address, reason);
        }
    }
}
