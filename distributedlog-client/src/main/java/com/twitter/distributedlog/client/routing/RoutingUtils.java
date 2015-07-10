package com.twitter.distributedlog.client.routing;

import com.twitter.common.zookeeper.ServerSet;

import java.net.SocketAddress;

/**
 * Utils for routing services
 */
public class RoutingUtils {

    private static final int NUM_CONSISTENT_HASH_REPLICAS = 997;

    /**
     * Building routing service from <code>finagleNameStr</code>
     *
     * @param finagleNameStr
     *          finagle name str of a service
     * @return routing service builder
     */
    public static RoutingService.Builder buildRoutingService(String finagleNameStr) {
        if (!finagleNameStr.startsWith("serverset!") && !finagleNameStr.startsWith("inet!")) {
            // We only support serverset based names at the moment
            throw new UnsupportedOperationException("Finagle Name format not supported for name: " + finagleNameStr);
        }
        return buildRoutingService(new NameServerSet(finagleNameStr), true);
    }

    /**
     * Building routing service from <code>serverSet</code>.
     *
     * @param serverSet
     *          server set of a service
     * @return routing service builder
     */
    public static RoutingService.Builder buildRoutingService(ServerSet serverSet) {
        return buildRoutingService(serverSet, false);
    }

    /**
     * Building routing service from <code>address</code>.
     *
     * @param address
     *          host to route the requests
     * @return routing service builder
     */
    public static RoutingService.Builder buildRoutingService(SocketAddress address) {
        return SingleHostRoutingService.newBuilder().address(address);
    }

    /**
     * Build routing service builder of a routing service <code>routingService</code>.
     *
     * @param routingService
     *          routing service to provide
     * @return routing service builder
     */
    public static RoutingService.Builder buildRoutingService(RoutingService routingService) {
        return new RoutingServiceProvider(routingService);
    }

    private static RoutingService.Builder buildRoutingService(ServerSet serverSet,
                                                              boolean resolveFromName) {
        return ConsistentHashRoutingService.newBuilder()
                .serverSet(serverSet)
                .resolveFromName(resolveFromName)
                .numReplicas(NUM_CONSISTENT_HASH_REPLICAS);
    }

}
