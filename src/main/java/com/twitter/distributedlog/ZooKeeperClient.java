// =================================================================================================
// Copyright 2011 Twitter, Inc.
// -------------------------------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file, or at:
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================================================

package com.twitter.distributedlog;

import com.twitter.distributedlog.zk.ZKWatcherManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Manages a connection to a ZooKeeper cluster.
 */
public class ZooKeeperClient {

    public static interface Credentials {

        Credentials NONE = new Credentials() {
            @Override
            public void authenticate(ZooKeeper zooKeeper) {
                // noop
            }
        };

        void authenticate(ZooKeeper zooKeeper);
    }

    public static class DigestCredentials implements Credentials {

        String username;
        String password;

        public DigestCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public void authenticate(ZooKeeper zooKeeper) {
            zooKeeper.addAuthInfo("digest", String.format("%s:%s", username, password).getBytes(UTF_8));
        }
    }

    public interface ZooKeeperSessionExpireNotifier {
        void notifySessionExpired();
    }

    /**
     * Indicates an error connecting to a zookeeper cluster.
     */
    public static class ZooKeeperConnectionException extends IOException {
        private static final long serialVersionUID = 6682391687004819361L;

        public ZooKeeperConnectionException(String message) {
            super(message);
        }

        public ZooKeeperConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class.getName());

    private final String name;
    private final int sessionTimeoutMs;
    private final int defaultConnectionTimeoutMs;
    private final String zooKeeperServers;
    // GuardedBy "this", but still volatile for tests, where we want to be able to see writes
    // made from within long synchronized blocks.
    private volatile ZooKeeper zooKeeper = null;
    private final RetryPolicy retryPolicy;
    private final StatsLogger statsLogger;
    private final int retryThreadCount;
    private final double requestRateLimit;
    private final Credentials credentials;
    private volatile boolean authenticated = false;

    private boolean closed = false;

    final Set<Watcher> watchers = new CopyOnWriteArraySet<Watcher>();

    // watcher manager to manage watchers
    private final ZKWatcherManager watcherManager;

    /**
     * Creates an unconnected client that will lazily attempt to connect on the first call to
     * {@link #get}.  All successful connections will be authenticated with the given
     * {@code credentials}.
     *
     * @param sessionTimeoutMs
     *          ZK session timeout in milliseconds
     * @param connectionTimeoutMs
     *          ZK connection timeout in milliseconds
     * @param zooKeeperServers
     *          the set of servers forming the ZK cluster
     */
    ZooKeeperClient(int sessionTimeoutMs, int connectionTimeoutMs, String zooKeeperServers) {
        this("default", sessionTimeoutMs, connectionTimeoutMs, zooKeeperServers, null, NullStatsLogger.INSTANCE, 1, 0,
             Credentials.NONE);
    }

    ZooKeeperClient(String name, int sessionTimeoutMs, int connectionTimeoutMs, String zooKeeperServers,
                    RetryPolicy retryPolicy, StatsLogger statsLogger, int retryThreadCount, double requestRateLimit,
                    Credentials credentials) {
        this.name = name;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.zooKeeperServers = zooKeeperServers;
        this.defaultConnectionTimeoutMs = connectionTimeoutMs;
        this.retryPolicy = retryPolicy;
        this.statsLogger = statsLogger;
        this.retryThreadCount = retryThreadCount;
        this.requestRateLimit = requestRateLimit;
        this.credentials = credentials;
        this.watcherManager = ZKWatcherManager.newBuilder()
                .name(name)
                .statsLogger(statsLogger.scope("watcher_manager"))
                .build();
    }

    public List<ACL> getDefaultACL() {
        if (Credentials.NONE == credentials) {
            return ZooDefs.Ids.OPEN_ACL_UNSAFE;
        } else {
            return DistributedLogConstants.EVERYONE_READ_CREATOR_ALL;
        }
    }

    public ZKWatcherManager getWatcherManager() {
        return watcherManager;
    }

    /**
     * Returns the current active ZK connection or establishes a new one if none has yet been
     * established or a previous connection was disconnected or had its session time out.
     *
     * @return a connected ZooKeeper client
     * @throws ZooKeeperConnectionException if there was a problem connecting to the ZK cluster
     * @throws InterruptedException if interrupted while waiting for a connection to be established
     * @throws TimeoutException if a connection could not be established within the configured
     * session timeout
     */
    public synchronized ZooKeeper get()
        throws ZooKeeperConnectionException, InterruptedException {

        try {
            FailpointUtils.checkFailPoint(FailpointUtils.FailPointName.FP_ZooKeeperConnectionLoss);
        } catch (IOException ioe) {
            throw new ZooKeeperConnectionException("Client " + name + " failed on establishing zookeeper connection", ioe);
        }

        // This indicates that the client was explictly closed
        if (closed) {
            throw new ZooKeeperConnectionException("Client " + name + " has already been closed");
        }

        if (zooKeeper == null) {
            zooKeeper = buildZooKeeper();
        }

        // In case authenticate throws an exception, the caller can try to recover the client by
        // calling get again.
        if (!authenticated) {
            credentials.authenticate(zooKeeper);
            authenticated = true;
        }

        return zooKeeper;
    }

    private ZooKeeper buildZooKeeper()
        throws ZooKeeperConnectionException, InterruptedException {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                    case None:
                        switch (event.getState()) {
                            case Expired:
                                if (null == retryPolicy) {
                                    LOG.info("ZooKeeper {}' session expired. Event: {}", name, event);
                                    closeInternal();
                                }
                                authenticated = false;
                                break;
                            case Disconnected:
                                if (null == retryPolicy) {
                                    LOG.info("ZooKeeper {} is disconnected from zookeeper now," +
                                            " but it is OK unless we received EXPIRED event.", name);
                                }
                                // Mark as not authenticated if expired or disconnected. In both cases
                                // we lose any attached auth info. Relying on Expired/Disconnected is
                                // sufficient since all Expired/Disconnected events are processed before
                                // all SyncConnected events, and the underlying member is not updated until
                                // SyncConnected is received.
                                authenticated = false;
                                break;
                            default:
                                break;
                        }
                }

                try {
                    for (Watcher watcher : watchers) {
                        try {
                            watcher.process(event);
                        } catch (Throwable t) {
                            LOG.warn("Encountered unexpected exception from watcher {} : ", watcher, t);
                        }
                    }
                } catch (Throwable t) {
                    LOG.warn("Encountered unexpected exception when firing watched event {} : ", event, t);
                }
            }
        };

        Set<Watcher> watchers = new HashSet<Watcher>();
        watchers.add(watcher);

        ZooKeeper zk;
        try {
            RetryPolicy opRetryPolicy = null == retryPolicy ?
                    new BoundExponentialBackoffRetryPolicy(sessionTimeoutMs, sessionTimeoutMs, 0) : retryPolicy;
            RetryPolicy connectRetryPolicy = null == retryPolicy ?
                    new BoundExponentialBackoffRetryPolicy(sessionTimeoutMs, sessionTimeoutMs, 0) :
                    new BoundExponentialBackoffRetryPolicy(sessionTimeoutMs, sessionTimeoutMs, Integer.MAX_VALUE);
            zk = org.apache.bookkeeper.zookeeper.ZooKeeperClient.newBuilder()
                    .connectString(zooKeeperServers)
                    .sessionTimeoutMs(sessionTimeoutMs)
                    .watchers(watchers)
                    .operationRetryPolicy(opRetryPolicy)
                    .connectRetryPolicy(connectRetryPolicy)
                    .statsLogger(statsLogger)
                    .retryThreadCount(retryThreadCount)
                    .requestRateLimit(requestRateLimit)
                    .build();
        } catch (KeeperException e) {
            throw new ZooKeeperConnectionException("Problem connecting to servers: " + zooKeeperServers, e);
        } catch (IOException e) {
            throw new ZooKeeperConnectionException("Problem connecting to servers: " + zooKeeperServers, e);
        }
        return zk;
    }

    /**
     * Clients that need to re-establish state after session expiration can register an
     * {@code onExpired} command to execute.
     *
     * @param onExpired the {@code Command} to register
     * @return the new {@link Watcher} which can later be passed to {@link #unregister} for
     *         removal.
     */
    public Watcher registerExpirationHandler(final ZooKeeperSessionExpireNotifier onExpired) {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None && event.getState() == KeeperState.Expired) {
                    try {
                        onExpired.notifySessionExpired();
                    } catch (Exception exc) {
                        // do nothing
                    }
                }
            }
        };
        register(watcher);
        return watcher;
    }

    /**
     * Clients that need to register a top-level {@code Watcher} should do so using this method.  The
     * registered {@code watcher} will remain registered across re-connects and session expiration
     * events.
     *
     * @param watcher the {@code Watcher to register}
     */
    public void register(Watcher watcher) {
        if (null != watcher) {
            watchers.add(watcher);
        }
    }

    /**
     * Clients can attempt to unregister a top-level {@code Watcher} that has previously been
     * registered.
     *
     * @param watcher the {@code Watcher} to unregister as a top-level, persistent watch
     * @return whether the given {@code Watcher} was found and removed from the active set
     */
    public boolean unregister(Watcher watcher) {
        return null != watcher && watchers.remove(watcher);
    }

    /**
     * Closes the current connection if any expiring the current ZooKeeper session.  Any subsequent
     * calls to this method will no-op until the next successful {@link #get}.
     */
    public synchronized void closeInternal() {
        if (zooKeeper != null) {
            try {
                LOG.info("Closing zookeeper client {}.", name);
                zooKeeper.close();
                LOG.info("Closed zookeeper client {}.", name);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted trying to close zooKeeper {} : ", name, e);
            } finally {
                zooKeeper = null;
            }
        }
    }

    /**
     * Closes the the underlying zookeeper instance.
     * Subsequent attempts to {@link #get} will fail
     */
    public synchronized void close() {
        if (closed) {
            return;
        }
        LOG.info("Close zookeeper client {}.", name);
        closeInternal();
        closed = true;
    }
}
