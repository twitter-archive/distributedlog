package com.twitter.distributedlog.client.routing;

import com.google.common.collect.ImmutableSet;
import com.twitter.distributedlog.service.DLSocketAddress;

/**
 * Watch on server set changes
 */
public interface ServerSetWatcher {

    public static class MonitorException extends Exception {

        private static final long serialVersionUID = 392751505154339548L;

        public MonitorException(String msg) {
            super(msg);
        }

        public MonitorException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /**
     * An interface to an object that is interested in receiving notification whenever the host set
     * changes.
     */
    public static interface ServerSetMonitor {

        /**
         * Called when either the available set of services changes (when a service dies or a new
         * instance comes on-line) or when an existing service advertises a status or health change.
         *
         * @param hostSet the current set of available ServiceInstances
         */
        void onChange(ImmutableSet<DLSocketAddress> hostSet);
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
    void watch(final ServerSetMonitor monitor) throws MonitorException;
}
