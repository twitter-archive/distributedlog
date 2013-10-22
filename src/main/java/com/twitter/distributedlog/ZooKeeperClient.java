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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


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
    public class ZooKeeperConnectionException extends IOException {
        public ZooKeeperConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final class SessionState {
        private final long sessionId;
        private final byte[] sessionPasswd;

        private SessionState(long sessionId, byte[] sessionPasswd) {
            this.sessionId = sessionId;
            this.sessionPasswd = sessionPasswd;
        }
    }

    private static final Logger LOG = Logger.getLogger(ZooKeeperClient.class.getName());

    private final int sessionTimeoutMs;
    private final int defaultConnectionTimeoutMs;
    private final String zooKeeperServers;
    // GuardedBy "this", but still volatile for tests, where we want to be able to see writes
    // made from within long synchronized blocks.
    private volatile ZooKeeper zooKeeper = null;
    private SessionState sessionState = null;
    private final AtomicInteger refCount;

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
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.zooKeeperServers = zooKeeperServers;
        this.defaultConnectionTimeoutMs = connectionTimeoutMs;
        this.refCount = new AtomicInteger(1);
    }

    /**
     * Increment reference on this client.
     *
     * @return reference count.
     */
    int addRef() {
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

        if (zooKeeper == null) {
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
                                    close();
                                    break;
                                case SyncConnected:
                                    connected.countDown();
                                    break;
                            }
                    }

                    synchronized (watchers) {
                        for (Watcher watcher : watchers) {
                            watcher.process(event);
                        }
                    }
                }
            };

            try {
                zooKeeper = (sessionState != null)
                    ? new ZooKeeper(zooKeeperServers, sessionTimeoutMs, watcher, sessionState.sessionId,
                    sessionState.sessionPasswd)
                    : new ZooKeeper(zooKeeperServers, sessionTimeoutMs, watcher);
            } catch (IOException e) {
                throw new ZooKeeperConnectionException(
                    "Problem connecting to servers: " + zooKeeperServers, e);
            }

            if (connectionTimeoutMs > 0) {
                if (!connected.await(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
                    close();
                    throw new TimeoutException("Timed out waiting for a ZK connection after "
                        + connectionTimeoutMs);
                }
            } else {
                try {
                    connected.await();
                } catch (InterruptedException ex) {
                    LOG.info("Interrupted while waiting to connect to zooKeeper");
                    close();
                    throw ex;
                }
            }

            sessionState = new SessionState(zooKeeper.getSessionId(), zooKeeper.getSessionPasswd());
        }
        return zooKeeper;
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
    public synchronized void close() {
        int refs = refCount.decrementAndGet();
        if (zooKeeper != null && refs == 0) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warning("Interrupted trying to close zooKeeper");
            } finally {
                zooKeeper = null;
                sessionState = null;
            }
        }
    }
}
