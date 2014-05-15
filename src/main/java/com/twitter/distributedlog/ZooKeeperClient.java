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

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Manages a connection to a ZooKeeper cluster.
 */
public class ZooKeeperClient {

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

    private final static class SessionState {
        private final long sessionId;
        private final byte[] sessionPasswd;

        private SessionState(long sessionId, byte[] sessionPasswd) {
            this.sessionId = sessionId;
            this.sessionPasswd = sessionPasswd;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class.getName());

    private final int sessionTimeoutMs;
    private final int defaultConnectionTimeoutMs;
    private final String zooKeeperServers;
    // GuardedBy "this", but still volatile for tests, where we want to be able to see writes
    // made from within long synchronized blocks.
    private volatile ZooKeeper zooKeeper = null;
    private SessionState sessionState = null;
    private final AtomicInteger refCount;
    private final RetryPolicy retryPolicy;
    private final StatsLogger statsLogger;
    private final int retryThreadCount;


    private final Set<Watcher> watchers = Collections.synchronizedSet(new HashSet<Watcher>());

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
        this(sessionTimeoutMs, connectionTimeoutMs, zooKeeperServers, null, NullStatsLogger.INSTANCE, 1);
    }

    ZooKeeperClient(int sessionTimeoutMs, int connectionTimeoutMs, String zooKeeperServers,
                    RetryPolicy retryPolicy, StatsLogger statsLogger, int retryThreadCount) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.zooKeeperServers = zooKeeperServers;
        this.defaultConnectionTimeoutMs = connectionTimeoutMs;
        this.refCount = new AtomicInteger(1);
        this.retryPolicy = retryPolicy;
        this.statsLogger = statsLogger;
        this.retryThreadCount = retryThreadCount;
    }

    /**
     * Increment reference on this client.
     *
     * @return reference count.
     */
    public int addRef() {
        return refCount.incrementAndGet();
    }

    /**
     * Returns the current active ZK connection or establishes a new one if none has yet been
     * established or a previous connection was disconnected or had its session time out.  This method
     * will attempt to re-use sessions when possible.  Equivalent to:
     * <pre>get(Amount.of(0L, ...)</pre>.
     *
     * @return a connected ZooKeeper client
     * @throws ZooKeeperConnectionException if there was a problem connecting to the ZK cluster
     * @throws InterruptedException if interrupted while waiting for a connection to be established
     */
    public synchronized ZooKeeper get() throws ZooKeeperConnectionException, InterruptedException {
        try {
            return get(defaultConnectionTimeoutMs);
        } catch (TimeoutException e) {
            InterruptedException interruptedException =
                new InterruptedException("Got an unexpected TimeoutException for 0 wait");
            interruptedException.initCause(e);
            throw interruptedException;
        }
    }

    /**
     * Returns the current active ZK connection or establishes a new one if none has yet been
     * established or a previous connection was disconnected or had its session time out.  This
     * method will attempt to re-use sessions when possible.
     *
     * @param connectionTimeoutMs the maximum amount of time to wait for the connection to the ZK
     * cluster to be established; 0 to wait forever
     * @return a connected ZooKeeper client
     * @throws ZooKeeperConnectionException if there was a problem connecting to the ZK cluster
     * @throws InterruptedException if interrupted while waiting for a connection to be established
     * @throws TimeoutException if a connection could not be established within the configured
     * session timeout
     */
    public synchronized ZooKeeper get(int connectionTimeoutMs)
        throws ZooKeeperConnectionException, InterruptedException, TimeoutException {

        // This indicates that the client was explictly closed
        if (0 == refCount.get()) {
            throw new ZooKeeperConnectionException("Client has already been closed");
        }

        if (zooKeeper == null) {
            if (null != retryPolicy) {
                zooKeeper = buildRetryableZooKeeper(connectionTimeoutMs);
            } else {
                zooKeeper = buildZooKeeper(connectionTimeoutMs);
            }
        }
        return zooKeeper;
    }

    private ZooKeeper buildRetryableZooKeeper(int connectionTimeout)
        throws ZooKeeperConnectionException, InterruptedException, TimeoutException {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                for (Watcher watcher : watchers) {
                    watcher.process(event);
                }
            }
        };
        Set<Watcher> watchers = new HashSet<Watcher>();
        watchers.add(watcher);

        ZooKeeper zk;
        try {
            zk = org.apache.bookkeeper.zookeeper.ZooKeeperClient.createConnectedZooKeeperClient(
                    zooKeeperServers, sessionTimeoutMs, watchers, retryPolicy, statsLogger, retryThreadCount);
        } catch (KeeperException e) {
            throw new ZooKeeperConnectionException("Problem connecting to servers: " + zooKeeperServers, e);
        } catch (IOException e) {
            throw new ZooKeeperConnectionException("Problem connecting to servers: " + zooKeeperServers, e);
        }
        return zk;
    }

    private ZooKeeper buildZooKeeper(int connectionTimeoutMs)
        throws ZooKeeperConnectionException, InterruptedException, TimeoutException {
        final CountDownLatch connected = new CountDownLatch(1);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                    // Guard the None type since this watch may be used as the default watch on calls by
                    // the client outside our control.
                    case None:
                        switch (event.getState()) {
                            case Expired:
                                LOG.info("Zookeeper session expired. Event: " + event);
                                closeInternal();
                                break;
                            case SyncConnected:
                                connected.countDown();
                                break;
                            default:
                                break;
                        }
                }

                for (Watcher watcher : watchers) {
                    watcher.process(event);
                }
            }
        };

        ZooKeeper zk;
        try {
            zk = (sessionState != null)
                ? new ZooKeeper(zooKeeperServers, sessionTimeoutMs, watcher, sessionState.sessionId,
                sessionState.sessionPasswd)
                : new ZooKeeper(zooKeeperServers, sessionTimeoutMs, watcher);
        } catch (IOException e) {
            throw new ZooKeeperConnectionException(
                "Problem connecting to servers: " + zooKeeperServers, e);
        }

        if (connectionTimeoutMs > 0) {
            if (!connected.await(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
                closeInternal();
                throw new TimeoutException("Timed out waiting for a ZK connection after "
                    + connectionTimeoutMs);
            }
        } else {
            try {
                connected.await();
            } catch (InterruptedException ex) {
                LOG.info("Interrupted while waiting to connect to zooKeeper");
                closeInternal();
                throw ex;
            }
        }

        sessionState = new SessionState(zk.getSessionId(), zk.getSessionPasswd());
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
        watchers.add(watcher);
    }

    /**
     * Clients can attempt to unregister a top-level {@code Watcher} that has previously been
     * registered.
     *
     * @param watcher the {@code Watcher} to unregister as a top-level, persistent watch
     * @return whether the given {@code Watcher} was found and removed from the active set
     */
    public boolean unregister(Watcher watcher) {
        return watchers.remove(watcher);
    }

    /**
     * Closes the current connection if any expiring the current ZooKeeper session.  Any subsequent
     * calls to this method will no-op until the next successful {@link #get}.
     */
    public synchronized void closeInternal() {
        if (zooKeeper != null) {
            try {
                LOG.info("Closing zookeeper client.");
                zooKeeper.close();
                LOG.info("Closed zookeeper client.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted trying to close zooKeeper");
            } finally {
                zooKeeper = null;
                sessionState = null;
            }
        }
    }

    /**
     * Closes the connection when the reference count drops to zero
     * Subsequent attempts to {@link #get} will fail
     */
    public synchronized void close() {
        int refs = refCount.decrementAndGet();
        LOG.info("Close zookeeper client : ref = {}.", refs);
        if (refs == 0) {
            closeInternal();
        }
    }
}
