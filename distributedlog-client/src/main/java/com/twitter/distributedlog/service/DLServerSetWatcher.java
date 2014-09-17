package com.twitter.distributedlog.service;

import java.net.InetSocketAddress;
import java.util.Map;

import com.google.common.collect.ImmutableSet;

import com.twitter.common.base.Command;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.thrift.ServiceInstance;
import com.twitter.thrift.Status;

public class DLServerSetWatcher {
    private final ServerSet serverSet;
    private final boolean resolvedFromName;

    public DLServerSetWatcher(ServerSet serverSet, boolean resolvedFromName) {
        this.serverSet = serverSet;
        this.resolvedFromName = resolvedFromName;
    }

    /**
     * Registers a monitor to receive change notices for this server set as long as this jvm process
     * is alive.  Blocks until the initial server set can be gathered and delivered to the monitor.
     * The monitor will be notified if the membership set or parameters of existing members have
     * changed.
     *
     * @param monitor the server set monitor to call back when the host set changes
     * @return A command which, when executed, will stop monitoring the host set.
     * @throws MonitorException if there is a problem monitoring the host set
     */
    public Command watch(final DLHostChangeMonitor<ServiceInstance> monitor) throws DynamicHostSet.MonitorException {
        return serverSet.watch(new DynamicHostSet.HostChangeMonitor<ServiceInstance>() {
            @Override
            public void onChange(ImmutableSet<ServiceInstance> hostSet) {
                monitor.onChange(hostSet, resolvedFromName);
            }
        });
    }

    /**
     * An interface to an object that is interested in receiving notification whenever the host set
     * changes.
     */
    public static interface DLHostChangeMonitor<ServiceInstance> {

        /**
         * Called when either the available set of services changes (when a service dies or a new
         * instance comes on-line) or when an existing service advertises a status or health change.
         *
         * @param hostSet the current set of available ServiceInstances
         */
        void onChange(ImmutableSet<com.twitter.thrift.ServiceInstance> hostSet, boolean resolvedFromName);
    }

}
