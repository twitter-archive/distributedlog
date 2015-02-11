package com.twitter.distributedlog.service;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.NoBrokersAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

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
        public RegionsRoutingService build() {
            Preconditions.checkNotNull(_routingServiceBuilders, "No routing service builder provided.");
            Preconditions.checkNotNull(_resolver, "No region resolver provided.");
            RoutingService[] services = new RoutingService[_routingServiceBuilders.length];
            for (int i = 0; i < services.length; i++) {
                services[i] = _routingServiceBuilders[i].build();
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
    public SocketAddress getHost(String key, SocketAddress previousAddr)
            throws NoBrokersAvailableException {
        return getHost(key, previousAddr, StatusCode.FOUND);
    }

    @Override
    public SocketAddress getHost(String key, SocketAddress previousAddr,
                                 StatusCode previousCode)
            throws NoBrokersAvailableException {
        String previousRegion = null;
        if (null != previousAddr) {
            previousRegion = regionResolver.resolveRegion(previousAddr);
        }
        for (RoutingService service : routingServices) {
            try {
                SocketAddress addr = service.getHost(key, previousAddr);
                if (StatusCode.REGION_UNAVAILABLE.equals(previousCode)) {
                    // current region is unavailable
                    String region = regionResolver.resolveRegion(addr);
                    if (Objects.equal(region, previousRegion)) {
                        continue;
                    }
                }
                if (!Objects.equal(addr, previousAddr)) {
                    return addr;
                }
            } catch (NoBrokersAvailableException nbae) {
                // if there isn't broker available in current service, try next service.
                logger.debug("No brokers available in region {} : ", service, nbae);
            }
        }
        throw new NoBrokersAvailableException("No host found for " + key + ", previous : " + previousAddr);
    }

    @Override
    public void removeHost(SocketAddress address, Throwable reason) {
        for (RoutingService service : routingServices) {
            service.removeHost(address, reason);
        }
    }
}
