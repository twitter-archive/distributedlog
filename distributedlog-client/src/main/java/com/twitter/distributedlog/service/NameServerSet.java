package com.twitter.distributedlog.service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.common.base.Command;
import com.twitter.common.base.Commands;
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.finagle.Addr;
import com.twitter.finagle.Name;
import com.twitter.finagle.Resolver$;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import com.twitter.thrift.Status;

class NameServerSet implements ServerSet {

    static final Logger logger = LoggerFactory.getLogger(NameServerSet.class);

    private volatile Set<HostChangeMonitor<ServiceInstance>> watchers = new HashSet<HostChangeMonitor<ServiceInstance>>();
    private volatile ImmutableSet<ServiceInstance> hostSet = ImmutableSet.of();

    public NameServerSet(String nameStr) {
        Name name = Resolver$.MODULE$.eval(nameStr);

        if (name instanceof Name.Bound) {
            ((Name.Bound)name).addr().changes().respond(new AbstractFunction1<Addr, BoxedUnit>() {
                @Override
                public BoxedUnit apply(Addr varAddr) {
                    return NameServerSet.this.respondToChanges(varAddr);
                }
            });

        } else {
            throw new UnsupportedOperationException("NameServerSet only supports Name.Bound");
        }

    }

    private ServiceInstance socketAddressToServiceInstance(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            Endpoint endpoint = new Endpoint(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
            HashMap<String, Endpoint> map = new HashMap<String, Endpoint>();
            map.put("thrift", endpoint);
            return new ServiceInstance(
                endpoint,
                map,
                Status.ALIVE);
        } else {
            throw new UnsupportedOperationException("invalid socket address: " + socketAddress);
        }
    }


    private BoxedUnit respondToChanges(Addr addr) {
        ImmutableSet<ServiceInstance> oldHostSet = ImmutableSet.copyOf(hostSet);

        ImmutableSet<ServiceInstance> newHostSet = oldHostSet;

        if (addr instanceof Addr.Bound) {
            scala.collection.immutable.Set<SocketAddress> socketAddresses = ((Addr.Bound)addr).addrs();
            scala.collection.Iterator<SocketAddress> socketAddressesIterator = socketAddresses.toIterator();
            HashSet<ServiceInstance> serviceInstances = new HashSet<ServiceInstance>();
            while (socketAddressesIterator.hasNext()) {
                serviceInstances.add(socketAddressToServiceInstance(socketAddressesIterator.next()));
            }
            newHostSet = ImmutableSet.copyOf(serviceInstances);

        } else if (addr instanceof Addr.Failed) {
            logger.error("Name resolution failed", ((Addr.Failed)addr).cause());
            newHostSet = ImmutableSet.of();
        } else if (addr.toString().equals("Pending")) {
            newHostSet = oldHostSet;
        } else if (addr.toString().equals("Neg")) {
            newHostSet = ImmutableSet.of();
        } else {
            throw new UnsupportedOperationException("Invalid Addr type:" + addr.getClass().getName());
        }

        // Reference comparison is valid as the sets are immutable
        if (oldHostSet != newHostSet) {
            logger.info("NameServerSet updated: {} -> {}", hostSetToString(oldHostSet), hostSetToString(newHostSet));
            hostSet = newHostSet;
            synchronized (watchers) {
                for (HostChangeMonitor<ServiceInstance> watcher: watchers) {
                    watcher.onChange(newHostSet);
                }
            }

        }

        return BoxedUnit.UNIT;
    }


    private String hostSetToString(ImmutableSet<ServiceInstance> hostSet) {
        String result = "(";
        for (ServiceInstance serviceInstance : hostSet) {
            Endpoint endpoint = serviceInstance.getServiceEndpoint();
            result += String.format(" %s:%d", endpoint.getHost(), endpoint.getPort());
        }
        result += " )";

        return result;
    }


    /**
     * Attempts to join a server set for this logical service group.
     *
     * @param endpoint the primary service endpoint
     * @param additionalEndpoints and additional endpoints keyed by their logical name
     * @param status the current service status
     * @return an EndpointStatus object that allows the endpoint to adjust its status
     * @throws JoinException if there was a problem joining the server set
     * @throws InterruptedException if interrupted while waiting to join the server set
     * @deprecated The status field is deprecated. Please use {@link #join(java.net.InetSocketAddress, java.util.Map)}
     */
    @Override
    public EndpointStatus join(InetSocketAddress endpoint, Map<String, InetSocketAddress> additionalEndpoints, Status status) throws Group.JoinException, InterruptedException {
        throw new UnsupportedOperationException("NameServerSet does not support join");
    }

    /**
     * Attempts to join a server set for this logical service group.
     *
     * @param endpoint the primary service endpoint
     * @param additionalEndpoints and additional endpoints keyed by their logical name
     * @return an EndpointStatus object that allows the endpoint to adjust its status
     * @throws JoinException if there was a problem joining the server set
     * @throws InterruptedException if interrupted while waiting to join the server set
     */
    @Override
    public EndpointStatus join(InetSocketAddress endpoint, Map<String, InetSocketAddress> additionalEndpoints) throws Group.JoinException, InterruptedException {
        throw new UnsupportedOperationException("NameServerSet does not support join");
    }

    /**
     * Attempts to join a server set for this logical service group.
     *
     * @param endpoint the primary service endpoint
     * @param additionalEndpoints and additional endpoints keyed by their logical name
     * @param shardId Unique shard identifier for this member of the service.
     * @return an EndpointStatus object that allows the endpoint to adjust its status
     * @throws JoinException if there was a problem joining the server set
     * @throws InterruptedException if interrupted while waiting to join the server set
     */
    @Override
    public EndpointStatus join(InetSocketAddress endpoint, Map<String, InetSocketAddress> additionalEndpoints, int shardId) throws Group.JoinException, InterruptedException {
        throw new UnsupportedOperationException("NameServerSet does not support join");
    }

    /**
     * Registers a monitor to receive change notices for this server set as long as this jvm process
     * is alive.  Blocks until the initial server set can be gathered and delivered to the monitor.
     * The monitor will be notified if the membership set or parameters of existing members have
     * changed.
     *
     * @param monitor the server set monitor to call back when the host set changes
     * @throws com.twitter.common.net.pool.DynamicHostSet.MonitorException if there is a problem monitoring the host set
     * @deprecated Deprecated in favor of {@link #watch(com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor)}
     */
    @Deprecated
    @Override
    public void monitor(HostChangeMonitor<ServiceInstance> monitor) throws MonitorException {
        throw new UnsupportedOperationException("NameServerSet does not support monitor");
    }

    /**
     * Registers a monitor to receive change notices for this server set as long as this jvm process
     * is alive.  Blocks until the initial server set can be gathered and delivered to the monitor.
     * The monitor will be notified if the membership set or parameters of existing members have
     * changed.
     *
     * @param monitor the server set monitor to call back when the host set changes
     * @return A command which, when executed, will stop monitoring the host set.
     * @throws com.twitter.common.net.pool.DynamicHostSet.MonitorException if there is a problem monitoring the host set
     */
    @Override
    public Command watch(HostChangeMonitor<ServiceInstance> monitor) throws MonitorException {
        // First add the monitor to the watchers so that it does not miss any changes and invoke
        // the onChange method
        synchronized (watchers) {
            watchers.add(monitor);
        }
        monitor.onChange(hostSet);

        return Commands.NOOP; // Return value is not used
    }
}
