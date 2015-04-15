package com.twitter.distributedlog.lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.FailpointUtils;
import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.stats.OpStatsListener;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Throw;
import com.twitter.util.TimeoutException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A lock under a given zookeeper session. This is a one-time lock.
 * It is not reusable: if lock failed, if zookeeper session is expired, if #unlock is called,
 * it would be transitioned to expired or closed state.
 *
 * The Locking Procedure is described as below.
 *
 * <p>
 * 0. if it is an immediate lock, it would get lock waiters first. if the lock is already held
 *    by someone. it would fail immediately with {@link com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException}
 *    with current owner. if there is no lock waiters, it would start locking procedure from 1.
 * 1. prepare: create a sequential znode to identify the lock.
 * 2. check lock waiters: get all lock waiters to check after prepare. if it is the first waiter,
 *    claim the ownership; if it is not the first waiter, but first waiter was itself (same client id and same session id)
 *    claim the ownership too; otherwise, it would set watcher on its sibling and wait it to disappared.
 * </p>
 *
 *                      +-----------------+
 *                      |       INIT      | ------------------------------+
 *                      +--------+--------+                               |
 *                               |                                        |
 *                               |                                        |
 *                      +--------v--------+                               |
 *                      |    PREPARING    |----------------------------+  |
 *                      +--------+--------+                            |  |
 *                               |                                     |  |
 *                               |                                     |  |
 *                      +--------v--------+                            |  |
 *        +-------------|    PREPARED     |--------------+             |  |
 *        |             +-----^---------+-+              |             |  |
 *        |                   |  |      |                |             |  |
 *        |                   |  |      |                |             |  |
 *        |                   |  |      |                |             |  |
 * +------V-----------+       |  |      |       +--------v----------+  |  |
 * |     WAITING      |-------+  |      |       |    CLAIMED        |  |  |
 * +------+-----+-----+          |      |       +--+----------+-----+  |  |
 *        |     |                |      |          |        |          |  |
 *        |     |                |      |          |        |          |  |
 *        |     |                |      |          |        |          |  |
 *        |     |                |    +-v----------v----+   |          |  |
 *        |     +-------------------->|     EXPIRED     |   |          |  |
 *        |                      |    +--+--------------+   |          |  |
 *        |                      |       |                  |          |  |
 *        |                      |       |                  |          |  |
 *        |             +--------V-------V-+                |          |  |
 *        +------------>|     CLOSING      |<---------------+----------+--+
 *                      +------------------+
 *                               |
 *                               |
 *                               |
 *                      +--------V---------+
 *                      |     CLOSED       |
 *                      +------------------+
 */
class DistributedLock {

    static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);

    private static final String LOCK_PATH_PREFIX = "/member_";
    private static final String LOCK_PART_SEP = "_";

    public static String getLockPathPrefixV1(String lockPath) {
        // member_
        return lockPath + LOCK_PATH_PREFIX;
    }

    public static String getLockPathPrefixV2(String lockPath, String clientId) throws UnsupportedEncodingException {
        // member_<clientid>_
        return lockPath + LOCK_PATH_PREFIX + URLEncoder.encode(clientId, UTF_8.name()) + LOCK_PART_SEP;
    }

    public static String getLockPathPrefixV3(String lockPath, String clientId, long sessionOwner) throws UnsupportedEncodingException {
        // member_<clientid>_s<owner_session>_
        StringBuilder sb = new StringBuilder();
        sb.append(lockPath).append(LOCK_PATH_PREFIX).append(URLEncoder.encode(clientId, UTF_8.name())).append(LOCK_PART_SEP)
                .append("s").append(String.format("%10d", sessionOwner)).append(LOCK_PART_SEP);
        return sb.toString();
    }

    public static byte[] serializeClientId(String clientId) {
        return clientId.getBytes(UTF_8);
    }

    public static String deserializeClientId(byte[] data) {
        return new String(data, UTF_8);
    }

    public static String getLockIdFromPath(String path) {
        // We only care about our actual id since we want to compare ourselves to siblings.
        if (path.contains("/")) {
            return path.substring(path.lastIndexOf("/") + 1);
        } else {
            return path;
        }
    }

    static final Comparator<String> MEMBER_COMPARATOR = new Comparator<String>() {
        public int compare(String o1, String o2) {
            int l1 = parseMemberID(o1);
            int l2 = parseMemberID(o2);
            return l1 - l2;
        }
    };

    static enum State {
        INIT,      // initialized state
        PREPARING, // preparing to lock, but no lock node created
        PREPARED,  // lock node created
        CLAIMED,   // claim lock ownership
        WAITING,   // waiting for the ownership
        EXPIRED,   // lock is expired
        CLOSING,   // lock is being closed
        CLOSED,    // lock is closed
    }

    private final ZooKeeperClient zkClient;
    private final ZooKeeper zk;
    private final String lockPath;
    // Identify a unique lock
    private final Pair<String, Long> lockId;
    private volatile State lockState;
    private final DistributedLockContext lockContext;

    private final Promise<Void> acquireFuture;
    private String currentId;
    private String currentNode;
    private String watchedNode;
    private LockWatcher watcher;
    private final AtomicInteger epoch = new AtomicInteger(0);
    private final OrderedSafeExecutor lockStateExecutor;
    private final LockListener lockListener;
    private final long lockOpTimeout;

    private final OpStatsLogger tryStats;
    private final Counter tryTimeouts;
    private final OpStatsLogger unlockStats;

    /**
     * Satisfied once the lock is acquired. Since this is a one time lock we never reset the
     * value--successful completion indicates the lock was initially acquired succesfully. but
     * doesn't say anything about the current state of the lock.
     */
    protected Future<Void> getAcquireFuture() {
        return acquireFuture;
    }

    public DistributedLock(ZooKeeperClient zkClient, String lockPath, String clientId,
                           OrderedSafeExecutor lockStateExecutor, LockListener lockListener)
            throws IOException {
        this(zkClient, lockPath, clientId, lockStateExecutor, lockListener,
                DistributedLogConstants.LOCK_OP_TIMEOUT_DEFAULT * 1000, NullStatsLogger.INSTANCE,
                new DistributedLockContext());
    }

    /**
     * Creates a distributed lock using the given {@code zkClient} to coordinate locking.
     *
     * @param zkClient The ZooKeeper client to use.
     * @param lockPath The path used to manage the lock under.
     * @param clientId client id use for lock.
     * @param lockStateExecutor executor to execute all lock state changes.
     * @param lockListener listener on lock state change.
     * @param lockOpTimeout timeout of lock operations
     * @param statsLogger stats logger
     */
    public DistributedLock(ZooKeeperClient zkClient,
                           String lockPath,
                           String clientId,
                           OrderedSafeExecutor lockStateExecutor,
                           LockListener lockListener,
                           long lockOpTimeout,
                           StatsLogger statsLogger,
                           DistributedLockContext lockContext)
            throws IOException {
        this.zkClient = zkClient;
        try {
            this.zk = zkClient.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException zce) {
            throw new ZKException("Failed to get zookeeper client for lock " + lockPath,
                    KeeperException.Code.CONNECTIONLOSS);
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on getting zookeeper client for lock " + lockPath, e);
        }
        this.lockPath = lockPath;
        this.lockId = Pair.of(clientId, this.zk.getSessionId());
        this.lockContext = lockContext;
        this.lockStateExecutor = lockStateExecutor;
        this.lockListener = lockListener;
        this.lockState = State.INIT;
        this.lockOpTimeout = lockOpTimeout;

        this.tryStats = statsLogger.getOpStatsLogger("tryAcquire");
        this.tryTimeouts = statsLogger.getCounter("tryTimeouts");
        this.unlockStats = statsLogger.getOpStatsLogger("unlock");

        // Attach interrupt handler to acquire future so clients can abort the future.
        this.acquireFuture = new Promise<Void>(new com.twitter.util.Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable t) {
                // This will set the lock state to closed, and begin to cleanup the zk lock node.
                // We have to be careful not to block here since doing so blocks the ordered lock
                // state executor which can cause deadlocks depending on how futures are chained.
                DistributedLock.this.unlockAsync();
                // Note re. logging and exceptions: errors are already logged by unlockAsync.
                return null;
            }
        });
    }

    String getLockPath() {
        return this.lockPath;
    }

    @VisibleForTesting
    AtomicInteger getEpoch() {
        return epoch;
    }

    @VisibleForTesting
    State getLockState() {
        return lockState;
    }

    @VisibleForTesting
    Pair<String, Long> getLockId() {
        return lockId;
    }

    boolean isExpiredOrClosed() {
        return State.CLOSED == lockState || State.EXPIRED == lockState;
    }

    /**
     * Execute a lock action of a given <i>lockEpoch</i> in ordered safe way.
     *
     * @param lockEpoch
     *          lock epoch
     * @param func
     *          function to execute a lock action
     */
    protected void executeLockAction(final int lockEpoch, final LockAction func) {
        lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
            @Override
            public void safeRun() {
                if (DistributedLock.this.epoch.get() == lockEpoch) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} executing lock action '{}' under epoch {} for lock {}",
                                new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                    }
                    func.execute();
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} executed lock action '{}' under epoch {} for lock {}",
                                new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                    }
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} skipped executing lock action '{}' for lock {}, since epoch is changed from {} to {}.",
                                new Object[]{lockId, func.getActionName(), lockPath, lockEpoch, DistributedLock.this.epoch.get()});
                    }
                }
            }
        });
    }

    /**
     * Execute a lock action of a given <i>lockEpoch</i> in ordered safe way. If the lock action couln't be
     * executed due to epoch changed, fail the given <i>promise</i> with
     * {@link DistributedReentrantLock.EpochChangedException}
     *
     * @param lockEpoch
     *          lock epoch
     * @param func
     *          function to execute a lock action
     * @param promise
     *          promise
     */
    protected <T> void executeLockAction(final int lockEpoch, final LockAction func, final Promise<T> promise) {
        lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
            @Override
            public void safeRun() {
                int currentEpoch = DistributedLock.this.epoch.get();
                if (currentEpoch == lockEpoch) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} executed lock action '{}' under epoch {} for lock {}",
                                new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                    }
                    func.execute();
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} executed lock action '{}' under epoch {} for lock {}",
                                new Object[]{lockId, func.getActionName(), lockEpoch, lockPath});
                    }
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} skipped executing lock action '{}' for lock {}, since epoch is changed from {} to {}.",
                                new Object[]{lockId, func.getActionName(), lockPath, lockEpoch, currentEpoch});
                    }
                    promise.setException(new EpochChangedException(lockPath, lockEpoch, currentEpoch));
                }
            }
        });
    }

    /**
     * Parse member id generated by zookeeper from given <i>nodeName</i>
     *
     * @param nodeName
     *          lock node name
     * @return member id generated by zookeeper
     */
    static int parseMemberID(String nodeName) {
        int id = -1;
        String[] parts = nodeName.split("_");
        if (parts.length > 0) {
            try {
                id = Integer.parseInt(parts[parts.length - 1]);
            } catch (NumberFormatException nfe) {
                // make it to be MAX_VALUE, so the bad znode will never acquire the lock
                id = Integer.MAX_VALUE;
            }
        }
        return id;
    }

    /**
     * Get client id and its ephemeral owner.
     *
     * @param zkClient
     *          zookeeper client
     * @param lockPath
     *          lock path
     * @param nodeName
     *          node name
     * @return client id and its ephemeral owner.
     */
    static Future<Pair<String, Long>> asyncParseClientID(ZooKeeper zkClient, String lockPath, String nodeName) {
        String[] parts = nodeName.split("_");
        // member_<clientid>_s<owner_session>_
        if (4 == parts.length && parts[2].startsWith("s")) {
            long sessionOwner = Long.parseLong(parts[2].substring(1));
            String clientId;
            try {
                clientId = URLDecoder.decode(parts[1], UTF_8.name());
                return Future.value(Pair.of(clientId, sessionOwner));
            } catch (UnsupportedEncodingException e) {
                // if failed to parse client id, we have to get client id by zookeeper#getData.
            }
        }
        final Promise<Pair<String, Long>> promise = new Promise<Pair<String, Long>>();
        zkClient.getData(lockPath + "/" + nodeName, false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (KeeperException.Code.OK.intValue() != rc) {
                    promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                } else {
                    promise.setValue(Pair.of(deserializeClientId(data), stat.getEphemeralOwner()));
                }
            }
        }, null);
        return promise;
    }

    public Future<String> asyncTryLock(long timeout, TimeUnit unit) {
        final Promise<String> result = new Promise<String>();
        final boolean wait = DistributedLogConstants.LOCK_IMMEDIATE != timeout;
        if (wait) {
            asyncTryLock(wait, result);
        } else {
            // try to check locks first
            zk.getChildren(lockPath, null, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(final int rc, String path, Object ctx,
                                          final List<String> children, Stat stat) {
                    lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                        @Override
                        public void safeRun() {
                            if (State.INIT != lockState) {
                                result.setException(new LockStateChangedException(lockPath, lockId, State.INIT, lockState));
                                return;
                            }
                            if (KeeperException.Code.OK.intValue() != rc) {
                                result.setException(KeeperException.create(KeeperException.Code.get(rc)));
                                return;
                            }

                            FailpointUtils.checkFailPointNoThrow(FailpointUtils.FailPointName.FP_LockTryAcquire);

                            Collections.sort(children, MEMBER_COMPARATOR);
                            if (children.size() > 0) {
                                asyncParseClientID(zk, lockPath, children.get(0)).addEventListener(
                                        new FutureEventListener<Pair<String, Long>>() {
                                            @Override
                                            public void onSuccess(Pair<String, Long> owner) {
                                                checkOrClaimLockOwner(owner, result);
                                            }

                                            @Override
                                            public void onFailure(final Throwable cause) {
                                                lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                                                    @Override
                                                    public void safeRun() {
                                                        result.setException(cause);
                                                    }
                                                });
                                            }
                                        });
                            } else {
                                asyncTryLock(wait, result);
                            }
                        }
                    });
                }
            }, null);
        }
        return result;
    }

    private void checkOrClaimLockOwner(final Pair<String, Long> currentOwner,
                                       final Promise<String> result) {
        if (lockId.compareTo(currentOwner) != 0 && !lockContext.hasLockId(currentOwner)) {
            lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                @Override
                public void safeRun() {
                    result.setValue(currentOwner.getLeft());
                }
            });
            return;
        }
        // current owner is itself
        final int curEpoch = epoch.incrementAndGet();
        executeLockAction(curEpoch, new LockAction() {
            @Override
            public void execute() {
                if (State.INIT != lockState) {
                    result.setException(new LockStateChangedException(lockPath, lockId, State.INIT, lockState));
                    return;
                }
                asyncTryLock(false, result);
            }
            @Override
            public String getActionName() {
                return "claimOwnership(owner=" + currentOwner + ")";
            }
        }, result);
    }

    /**
     * Try lock. If it failed, it would cleanup its attempt.
     *
     * @param wait
     *          whether to wait for ownership.
     * @param result
     *          promise to satisfy with current lock owner
     */
    private void asyncTryLock(boolean wait, final Promise<String> result) {
        final Promise<String> lockResult = new Promise<String>();
        lockResult.addEventListener(new FutureEventListener<String>() {
            @Override
            public void onSuccess(String currentOwner) {
                result.setValue(currentOwner);
            }

            @Override
            public void onFailure(final Throwable lockCause) {
                // If tryLock failed due to state changed, we don't need to cleanup
                if (lockCause instanceof LockStateChangedException) {
                    LOG.info("skipping cleanup for {} at {} after encountering lock " +
                            "state change exception : ", new Object[] { lockId, lockPath, lockCause });
                    result.setException(lockCause);
                    return;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is cleaning up its lock state for {} due to : ",
                            new Object[] { lockId, lockPath, lockCause });
                }

                // If we encountered any exception we should cleanup
                Future<BoxedUnit> unlockResult = unlockAsync();
                unlockResult.addEventListener(new FutureEventListener<BoxedUnit>() {
                    @Override
                    public void onSuccess(BoxedUnit value) {
                        result.setException(lockCause);
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        result.setException(lockCause);
                    }
                });
            }
        });
        asyncTryLockWithoutCleanup(wait, lockResult);
    }

    /**
     * Try lock. If wait is true, it would wait and watch sibling to acquire lock when
     * the sibling is dead. <i>acquireFuture</i> will be notified either it locked successfully
     * or the lock failed. The promise will only satisfy with current lock owner.
     *
     * NOTE: the <i>promise</i> is only satisfied on <i>lockStateExecutor</i>, so any
     * transformations attached on promise will be executed in order.
     *
     * @param wait
     *          whether to wait for ownership.
     * @param promise
     *          promise to satisfy with current lock owner.
     */
    private void asyncTryLockWithoutCleanup(final boolean wait, final Promise<String> promise) {
        executeLockAction(epoch.get(), new LockAction() {
            @Override
            public void execute() {
                if (State.INIT != lockState) {
                    promise.setException(new LockStateChangedException(lockPath, lockId, State.INIT, lockState));
                    return;
                }
                lockState = State.PREPARING;

                final int curEpoch = epoch.incrementAndGet();
                watcher = new LockWatcher(curEpoch);
                // register watcher for session expires
                zkClient.register(watcher);
                // Encode both client id and session in the lock node
                String myPath;
                try {
                    // member_<clientid>_s<owner_session>_
                    myPath = getLockPathPrefixV3(lockPath, lockId.getLeft(), lockId.getRight());
                } catch (UnsupportedEncodingException uee) {
                    myPath = getLockPathPrefixV1(lockPath);
                }
                zk.create(myPath, serializeClientId(lockId.getLeft()), zkClient.getDefaultACL(), CreateMode.EPHEMERAL_SEQUENTIAL,
                        new AsyncCallback.StringCallback() {
                            @Override
                            public void processResult(final int rc, String path, Object ctx, final String name) {
                                executeLockAction(curEpoch, new LockAction() {
                                    @Override
                                    public void execute() {
                                        if (KeeperException.Code.OK.intValue() != rc) {
                                            KeeperException ke = KeeperException.create(KeeperException.Code.get(rc));
                                            promise.setException(ke);
                                            return;
                                        }

                                        if (FailpointUtils.checkFailPointNoThrow(FailpointUtils.FailPointName.FP_LockTryCloseRaceCondition)) {
                                            lockState = State.CLOSED;
                                        }

                                        currentNode = name;
                                        currentId = getLockIdFromPath(currentNode);
                                        LOG.trace("{} received member id for lock {} : ", lockId, currentId);

                                        if (lockState == State.EXPIRED || lockState == State.CLOSING || lockState == State.CLOSED) {
                                            // Delete node attempt may have come after PREPARING but before create node, in which case
                                            // we'd be left with a dangling node unless we clean up.
                                            Promise<BoxedUnit> deletePromise = new Promise<BoxedUnit>();
                                            deleteLockNode(deletePromise);
                                            deletePromise.ensure(new Function0<BoxedUnit>() {
                                                public BoxedUnit apply() {
                                                    promise.setException(new LockClosedException(lockPath, lockId, lockState));
                                                    return BoxedUnit.UNIT;
                                                }
                                            });
                                            return;
                                        }

                                        lockState = State.PREPARED;
                                        checkLockOwnerAndWaitIfPossible(watcher, wait, promise);
                                    }

                                    @Override
                                    public String getActionName() {
                                        return "postPrepare(wait=" + wait + ")";
                                    }
                                });
                            }
                        }, null);
            }
            @Override
            public String getActionName() {
                return "prepare(wait=" + wait + ")";
            }
        }, promise);
    }

    public void tryLock(long timeout, TimeUnit unit) throws LockingException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        Future<String> tryFuture = asyncTryLock(timeout, unit);
        String currentOwner = waitForTry(stopwatch, tryFuture);
        boolean acquired = waitForAcquire(getAcquireFuture(), timeout, unit);
        if (!acquired) {
            throw new OwnershipAcquireFailedException(lockPath, currentOwner);
        }
    }

    synchronized String waitForTry(Stopwatch stopwatch, Future<String> tryFuture) throws LockingException {
        boolean success = false;
        boolean stateChanged = false;
        String currentOwner;
        try {
            currentOwner = Await.result(tryFuture, Duration.fromMilliseconds(lockOpTimeout));
            success = true;
        } catch (LockStateChangedException ex) {
            stateChanged = true;
            throw ex;
        } catch (LockingException ex) {
            throw ex;
        } catch (TimeoutException toe) {
            tryTimeouts.inc();
            throw new LockingException(lockPath, "Timeout during try phase of lock acquire", toe);
        } catch (Exception ex) {
            String message = getLockId() + " failed to lock " + lockPath;
            throw new LockingException(lockPath, message, ex);
        } finally {
            if (success) {
                tryStats.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            } else {
                tryStats.registerFailedEvent(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            }
            // This can only happen for a Throwable thats not an
            // Exception, i.e. an Error
            if (!success && !stateChanged) {
                unlock();
            }
        }
        return currentOwner;
    }

    public synchronized boolean waitForAcquire(Future<Void> acquireFuture, long timeout, TimeUnit unit)
            throws LockingException {
        try {
            if (DistributedLogConstants.LOCK_IMMEDIATE != timeout) {
                if (DistributedLogConstants.LOCK_TIMEOUT_INFINITE == timeout) {
                    Await.result(acquireFuture);
                }
                else {
                    Await.result(acquireFuture, Duration.fromMilliseconds(timeout));
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException toe) {
            LOG.debug("Failed on tryLocking {} in {} ms.", lockPath, unit.toMillis(timeout));
        } catch (Exception e) {
            LOG.error("Caught exception waiting for lock acquire", e);
        } finally {
            if (!isLockHeld()) {
                unlock();
            }
        }
        return isLockHeld();
    }

    Future<BoxedUnit> unlockAsync() {
        final Promise<BoxedUnit> promise = new Promise<BoxedUnit>();

        // Use lock executor here rather than lock action, because we want this opertaion to be applied
        // whether the epoch has changed or not. The member node is EPHEMERAL_SEQUENTIAL so there's no
        // risk of an ABA problem where we delete and recreate a node and then delete it again here.
        lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
            @Override
            public void safeRun() {
                acquireFuture.updateIfEmpty(new Throw(
                    new LockClosedException(lockPath, lockId, lockState)));
                unlockInternal(promise);
                promise.addEventListener(new OpStatsListener(unlockStats));
            }
        });

        return promise;
    }

    void unlock() {
        Future<BoxedUnit> unlockResult = unlockAsync();
        try {
            Await.result(unlockResult, Duration.fromMilliseconds(lockOpTimeout));
        } catch (TimeoutException toe) {
            // This shouldn't happen unless we lose a watch, and may result in a leaked lock.
            LOG.error("Timeout unlocking {} owned by {} : ", new Object[] { lockPath, lockId, toe });
        } catch (Exception e) {
            LOG.warn("{} failed to unlock {} : ", new Object[] { lockId, lockPath, e });
        }
    }

    // Lock State Changes (all state changes should be executed under a LockAction)

    private void claimOwnership(int lockEpoch) {
        lockState = State.CLAIMED;
        // clear previous lock ids
        lockContext.clearLockIds();
        // add current lock id
        lockContext.addLockId(lockId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Notify lock waiters on {} at {} : watcher epoch {}, lock epoch {}",
                    new Object[] { lockPath, System.currentTimeMillis(),
                            lockEpoch, DistributedLock.this.epoch.get() });
        }
        acquireFuture.setValue(null /* complete */);
    }

    public boolean isLockHeld() {
        return State.CLAIMED == lockState;
    }

    /**
     * NOTE: unlockInternal should only after try lock.
     */
    private void unlockInternal(final Promise<BoxedUnit> promise) {

        // already closed or expired, nothing to cleanup
        this.epoch.incrementAndGet();
        if (null != watcher) {
            this.zkClient.unregister(watcher);
        }

        if (State.CLOSED == lockState) {
            promise.setValue(BoxedUnit.UNIT);
            return;
        }

        LOG.info("Lock {} for {} is closed from state {}.",
                new Object[] { lockId, lockPath, lockState });

        if (State.INIT == lockState || State.EXPIRED == lockState) {
            // Nothing to cleanup if INIT (never tried) or EXPIRED (ephemeral node
            // auto-removed)
            lockState = State.CLOSED;
            promise.setValue(BoxedUnit.UNIT);
            return;
        }

        lockState = State.CLOSING;

        // In any other state, we should clean up the member node
        Promise<BoxedUnit> deletePromise = new Promise<BoxedUnit>();
        deleteLockNode(deletePromise);

        // Set the state to closed after we've cleaned up
        deletePromise.addEventListener(new FutureEventListener<BoxedUnit>() {
            @Override
            public void onSuccess(BoxedUnit complete) {
                lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        lockState = State.CLOSED;
                        promise.setValue(BoxedUnit.UNIT);
                    }
                });
            }
            @Override
            public void onFailure(Throwable cause) {
                // Delete failure is quite serious (causes lock leak) and should be
                // handled better (PUBSUB-6076)
                LOG.error("lock node delete failed {} {}", lockId, lockPath);
                promise.setValue(BoxedUnit.UNIT);
            }
        });
    }

    private void deleteLockNode(final Promise<BoxedUnit> promise) {
        if (null == currentNode) {
            promise.setValue(BoxedUnit.UNIT);
            return;
        }

        zk.delete(currentNode, -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(final int rc, final String path, Object ctx) {
                lockStateExecutor.submitOrdered(lockPath, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        if (KeeperException.Code.OK.intValue() == rc ||
                                KeeperException.Code.NONODE.intValue() == rc ||
                                KeeperException.Code.SESSIONEXPIRED.intValue() == rc) {

                            LOG.info("Deleted lock node {} for {} successfully.", path, lockId);
                        } else {
                            LOG.error("Failed on deleting lock node {} for {} : {}",
                                    new Object[] { path, lockId, KeeperException.Code.get(rc) });
                        }

                        FailpointUtils.checkFailPointNoThrow(FailpointUtils.FailPointName.FP_LockUnlockCleanup);
                        promise.setValue(BoxedUnit.UNIT);
                    }
                });
            }
        }, null);
    }

    /**
     * Handle session expired for lock watcher at epoch <i>lockEpoch</i>.
     *
     * @param lockEpoch
     *          lock epoch
     */
    private void handleSessionExpired(final int lockEpoch) {
        executeLockAction(lockEpoch, new LockAction() {
            @Override
            public void execute() {
                if (lockState == State.CLOSED || lockState == State.CLOSING) {
                    // Already fully closed, no need to process expire.
                    return;
                }

                boolean shouldNotifyLockListener = State.CLAIMED == lockState;
                lockState = State.EXPIRED;

                // remove the watcher
                if (null != watcher) {
                    zkClient.unregister(watcher);
                }

                // increment epoch to avoid any ongoing locking action
                DistributedLock.this.epoch.incrementAndGet();

                // if session expired, just notify the waiter. as the lock acquire doesn't succeed.
                // we don't even need to clean up the lock as the znode will disappear after session expired
                acquireFuture.updateIfEmpty(new Throw(
                        new LockSessionExpiredException(lockPath, lockId, lockState)));

                // session expired, ephemeral node is gone.
                currentNode = null;
                currentId = null;

                if (shouldNotifyLockListener) {
                    // if session expired after claimed, we need to notify the caller to re-lock
                    if (null != lockListener) {
                        lockListener.onExpired();
                    }
                }
            }

            @Override
            public String getActionName() {
                return "handleSessionExpired(epoch=" + lockEpoch + ")";
            }
        });
    }

    private void handleNodeDelete(int lockEpoch, final WatchedEvent event) {
        executeLockAction(lockEpoch, new LockAction() {
            @Override
            public void execute() {
                // The lock is either expired or closed
                if (State.WAITING != lockState) {
                    LOG.info("{} ignore watched node {} deleted event, since lock state has moved to {}.",
                            new Object[] { lockId, event.getPath(), lockState });
                    return;
                }
                lockState = State.PREPARED;
                // we don't need to wait and check the result, since:
                // 1) if it claimed the ownership, it would notify the waiters when claimed ownerships
                // 2) if it failed, it would also notify the waiters, the waiters would cleanup the state.
                checkLockOwnerAndWaitIfPossible(watcher, true);
            }

            @Override
            public String getActionName() {
                return "handleNodeDelete(path=" + event.getPath() + ")";
            }
        });
    }

    private Future<String> checkLockOwnerAndWaitIfPossible(final LockWatcher lockWatcher,
                                                           final boolean wait) {
        final Promise<String> promise = new Promise<String>();
        checkLockOwnerAndWaitIfPossible(lockWatcher, wait, promise);
        return promise;
    }

    /**
     * Check Lock Owner Phase 1 : Get all lock waiters.
     *
     * @param lockWatcher
     *          lock watcher.
     * @param wait
     *          whether to wait for ownership.
     * @param promise
     *          promise to satisfy with current lock owner
     */
    private void checkLockOwnerAndWaitIfPossible(final LockWatcher lockWatcher,
                                                 final boolean wait,
                                                 final Promise<String> promise) {
        zk.getChildren(lockPath, false, new AsyncCallback.Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                processLockWaiters(lockWatcher, wait, rc, children, promise);
            }
        }, null);
    }

    /**
     * Check Lock Owner Phase 2 : check all lock waiters to get current owner and wait for ownership if necessary.
     *
     * @param lockWatcher
     *          lock watcher.
     * @param wait
     *          whether to wait for ownership.
     * @param getChildrenRc
     *          result of getting all lock waiters
     * @param children
     *          current lock waiters.
     * @param promise
     *          promise to satisfy with current lock owner.
     */
    private void processLockWaiters(final LockWatcher lockWatcher,
                                    final boolean wait,
                                    final int getChildrenRc,
                                    final List<String> children,
                                    final Promise<String> promise) {
        executeLockAction(lockWatcher.epoch, new LockAction() {
            @Override
            public void execute() {
                if (State.PREPARED != lockState) { // e.g. lock closed or session expired after prepared
                    promise.setException(new LockStateChangedException(lockPath, lockId, State.PREPARED, lockState));
                    return;
                }

                if (KeeperException.Code.OK.intValue() != getChildrenRc) {
                    promise.setException(KeeperException.create(KeeperException.Code.get(getChildrenRc)));
                    return;
                }
                if (children.isEmpty()) {
                    LOG.error("Error, member list is empty for lock {}.", lockPath);
                    promise.setException(new UnexpectedException("Empty member list for lock " + lockPath));
                    return;
                }

                // sort the children
                Collections.sort(children, MEMBER_COMPARATOR);
                final String cid = currentId;
                final int memberIndex = children.indexOf(cid);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is the number {} member in the list.", cid, memberIndex);
                }
                // If we hold the lock
                if (memberIndex == 0) {
                    LOG.info("{} acquired the lock {}.", cid, lockPath);
                    claimOwnership(lockWatcher.epoch);
                    promise.setValue(cid);
                } else if (memberIndex > 0) { // we are in the member list but we didn't hold the lock
                    // get ownership of current owner
                    asyncParseClientID(zk, lockPath, children.get(0)).addEventListener(new FutureEventListener<Pair<String, Long>>() {
                        @Override
                        public void onSuccess(Pair<String, Long> currentOwner) {
                            watchLockOwner(lockWatcher, wait,
                                    cid, children.get(memberIndex - 1), children.get(0), currentOwner, promise);
                        }
                        @Override
                        public void onFailure(final Throwable cause) {
                            // ensure promise is satisfied in lock thread
                            executeLockAction(lockWatcher.epoch, new LockAction() {
                                @Override
                                public void execute() {
                                    promise.setException(cause);
                                }

                                @Override
                                public String getActionName() {
                                    return "handleFailureOnParseClientID(lockPath=" + lockPath + ")";
                                }
                            }, promise);
                        }
                    });
                } else {
                    LOG.error("Member {} doesn't exist in the members list {} for lock {}.",
                            new Object[]{ cid, children, lockPath});
                    promise.setException(
                            new UnexpectedException("Member " + cid + " doesn't exist in member list " +
                                    children + " for lock " + lockPath));
                }
            }

            @Override
            public String getActionName() {
                return "processLockWaiters(rc=" + getChildrenRc + ", waiters=" + children + ")";
            }
        }, promise);
    }

    /**
     * Check Lock Owner Phase 3: watch sibling node for lock ownership.
     *
     * @param lockWatcher
     *          lock watcher.
     * @param wait
     *          whether to wait for ownership.
     * @param myNode
     *          my lock node.
     * @param siblingNode
     *          my sibling lock node.
     * @param ownerNode
     *          owner lock node.
     * @param currentOwner
     *          current owner info.
     * @param promise
     *          promise to satisfy with current lock owner.
     */
    private void watchLockOwner(final LockWatcher lockWatcher,
                                final boolean wait,
                                final String myNode,
                                final String siblingNode,
                                final String ownerNode,
                                final Pair<String, Long> currentOwner,
                                final Promise<String> promise) {
        executeLockAction(lockWatcher.epoch, new LockAction() {
            @Override
            public void execute() {
                boolean shouldWatch;
                if (lockContext.hasLockId(currentOwner) && siblingNode.equals(ownerNode)) {
                    // if the current owner is the znode left from previous session
                    // we should watch it and claim ownership
                    shouldWatch = true;
                    LOG.info("LockWatcher {} for {} found its previous session {} held lock, watch it to claim ownership.",
                            new Object[] { myNode, lockPath, currentOwner });
                } else if (lockId.compareTo(currentOwner) == 0 && siblingNode.equals(ownerNode)) {
                    // I found that my sibling is the current owner with same lock id (client id & session id)
                    // It must be left by any race condition from same zookeeper client
                    // I would watch owner instead of sibling
                    shouldWatch = true;
                    LOG.info("LockWatcher {} for {} found itself {} already held lock at sibling node {}, watch it to claim ownership.",
                            new Object[]{myNode, lockPath, lockId, siblingNode});
                } else {
                    shouldWatch = wait;
                    if (wait) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Current LockWatcher for {} with ephemeral node {}, is waiting for {} to release lock at {}.",
                                    new Object[]{lockPath, myNode, siblingNode, System.currentTimeMillis()});
                        }
                    }
                }

                // watch sibling for lock ownership
                if (shouldWatch) {
                    watchedNode = String.format("%s/%s", lockPath, siblingNode);
                    zk.exists(watchedNode, lockWatcher, new AsyncCallback.StatCallback() {
                        @Override
                        public void processResult(final int rc, String path, Object ctx, final Stat stat) {
                            executeLockAction(lockWatcher.epoch, new LockAction() {
                                @Override
                                public void execute() {
                                    if (lockState != State.PREPARED) {
                                        promise.setException(new LockStateChangedException(lockPath, lockId, State.PREPARED, lockState));
                                        return;
                                    }

                                    if (KeeperException.Code.OK.intValue() == rc) {
                                        if (siblingNode.equals(ownerNode) &&
                                                (lockId.compareTo(currentOwner) == 0 || lockContext.hasLockId(currentOwner))) {
                                            // watch owner successfully
                                            LOG.info("LockWatcher {} claimed ownership for {} after set watcher on {}.",
                                                    new Object[]{ myNode, lockPath, ownerNode });
                                            claimOwnership(lockWatcher.epoch);
                                            promise.setValue(currentOwner.getLeft());
                                        } else {
                                            // watch sibling successfully
                                            lockState = State.WAITING;
                                            promise.setValue(currentOwner.getLeft());
                                        }
                                    } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                        // sibling just disappeared, it might be the chance to claim ownership
                                        checkLockOwnerAndWaitIfPossible(lockWatcher, wait, promise);
                                    } else {
                                        promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                                    }
                                }

                                @Override
                                public String getActionName() {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append("postWatchLockOwner(myNode=").append(myNode).append(", siblingNode=")
                                            .append(siblingNode).append(", ownerNode=").append(ownerNode).append(")");
                                    return sb.toString();
                                }
                            }, promise);
                        }
                    }, null);
                } else {
                    promise.setValue(currentOwner.getLeft());
                }
            }

            @Override
            public String getActionName() {
                StringBuilder sb = new StringBuilder();
                sb.append("watchLockOwner(myNode=").append(myNode).append(", siblingNode=")
                        .append(siblingNode).append(", ownerNode=").append(ownerNode).append(")");
                return sb.toString();
            }
        }, promise);
    }

    class LockWatcher implements Watcher {

        // Enforce a epoch number to avoid a race on canceling attempt
        final int epoch;

        LockWatcher(int epoch) {
            this.epoch = epoch;
        }

        @Override
        public void process(WatchedEvent event) {
            LOG.info("Received event {} from lock {} at {} : watcher epoch {}, lock epoch {}.",
                    new Object[] { event, lockPath, System.currentTimeMillis(), epoch, DistributedLock.this.epoch.get() });
            if (event.getType() == Watcher.Event.EventType.None) {
                switch (event.getState()) {
                    case SyncConnected:
                        break;
                    case Expired:
                        handleSessionExpired(epoch);
                        break;
                    default:
                        break;
                }
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                // this handles the case where we have aborted a lock and deleted ourselves but still have a
                // watch on the nextLowestNode. This is a workaround since ZK doesn't support unsub.
                if (!event.getPath().equals(watchedNode)) {
                    LOG.warn("{} (watching {}) ignored watched event from {} ",
                            new Object[] { lockId, watchedNode, event.getPath() });
                    return;
                }
                handleNodeDelete(epoch, event);
            } else {
                LOG.warn("Unexpected ZK event: {}", event.getType().name());
            }
        }

    }
}
