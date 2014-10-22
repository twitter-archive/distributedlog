package com.twitter.distributedlog.acl;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.thrift.AccessControlEntry;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ZooKeeper Based {@link com.twitter.distributedlog.acl.AccessControlManager}
 */
public class ZKAccessControlManager implements AccessControlManager, Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ZKAccessControlManager.class);

    private static final int ZK_RETRY_BACKOFF_MS = 500;

    protected final DistributedLogConfiguration conf;
    protected final ZooKeeperClient zkc;
    protected final String zkRootPath;
    protected final ScheduledExecutorService scheduledExecutorService;

    protected final ConcurrentMap<String, ZKAccessControl> streamEntries;
    protected ZKAccessControl defaultAccessControl;
    protected volatile boolean closed = false;

    public ZKAccessControlManager(DistributedLogConfiguration conf,
                                  ZooKeeperClient zkc,
                                  String zkRootPath,
                                  ScheduledExecutorService scheduledExecutorService) throws IOException {
        this.conf = conf;
        this.zkc = zkc;
        this.zkRootPath = zkRootPath;
        this.scheduledExecutorService = scheduledExecutorService;
        this.streamEntries = new ConcurrentHashMap<String, ZKAccessControl>();
        try {
            Await.result(fetchDefaultAccessControlEntry());
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                throw new DLInterruptedException("Interrupted on getting default access control entry for " + zkRootPath, t);
            } else if (t instanceof KeeperException) {
                throw new IOException("Encountered zookeeper exception on getting default access control entry for " + zkRootPath, t);
            } else if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException("Encountered unknown exception on getting access control entries for " + zkRootPath, t);
            }
        }

        try {
            Await.result(fetchAccessControlEntries());
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                throw new DLInterruptedException("Interrupted on getting access control entries for " + zkRootPath, t);
            } else if (t instanceof KeeperException) {
                throw new IOException("Encountered zookeeper exception on getting access control entries for " + zkRootPath, t);
            } else if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException("Encountered unknown exception on getting access control entries for " + zkRootPath, t);
            }
        }
    }

    protected AccessControlEntry getAccessControlEntry(String stream) {
        ZKAccessControl entry = streamEntries.get(stream);
        entry = null == entry ? defaultAccessControl : entry;
        return entry.getAccessControlEntry();
    }

    @Override
    public boolean allowWrite(String stream) {
        return !getAccessControlEntry(stream).isDenyWrite();
    }

    @Override
    public boolean allowTruncate(String stream) {
        return !getAccessControlEntry(stream).isDenyTruncate();
    }

    @Override
    public boolean allowDelete(String stream) {
        return !getAccessControlEntry(stream).isDenyDelete();
    }

    @Override
    public boolean allowAcquire(String stream) {
        return !getAccessControlEntry(stream).isDenyAcquire();
    }

    @Override
    public boolean allowRelease(String stream) {
        return !getAccessControlEntry(stream).isDenyRelease();
    }

    @Override
    public void close() {
        closed = true;
    }

    private Future<Void> fetchAccessControlEntries() {
        final Promise<Void> promise = new Promise<Void>();
        fetchAccessControlEntries(promise);
        return promise;
    }

    private void fetchAccessControlEntries(final Promise<Void> promise) {
        try {
            zkc.get().getChildren(zkRootPath, this, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                    if (KeeperException.Code.OK.intValue() != rc) {
                        promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                        return;
                    }
                    Set<String> streamsReceived = new HashSet<String>();
                    streamsReceived.addAll(children);
                    Set<String> streamsCached = streamEntries.keySet();
                    Set<String> streamsRemoved = Sets.difference(streamsCached, streamsReceived).immutableCopy();
                    for (String s : streamsRemoved) {
                        ZKAccessControl accessControl = streamEntries.remove(s);
                        if (null != accessControl) {
                            logger.info("Removed Access Control Entry for stream {} : {}", s, accessControl.getAccessControlEntry());
                        }
                    }
                    if (streamsReceived.isEmpty()) {
                        promise.setValue(null);
                        return;
                    }
                    final AtomicInteger numPendings = new AtomicInteger(streamsReceived.size());
                    final AtomicInteger numFailures = new AtomicInteger(0);
                    for (String s : streamsReceived) {
                        final String streamName = s;
                        ZKAccessControl.read(zkc, zkRootPath + "/" + streamName, null)
                                .addEventListener(new FutureEventListener<ZKAccessControl>() {

                                    @Override
                                    public void onSuccess(ZKAccessControl accessControl) {
                                        streamEntries.put(streamName, accessControl);
                                        logger.info("Added overrided access control for stream {} : {}", streamName, accessControl.getAccessControlEntry());
                                        complete();
                                    }

                                    @Override
                                    public void onFailure(Throwable cause) {
                                        if (cause instanceof KeeperException.NoNodeException) {
                                            streamEntries.remove(streamName);
                                        } else if (cause instanceof ZKAccessControl.CorruptedAccessControlException) {
                                            logger.warn("Access control is corrupted for stream {} @ {}, skipped it ...",
                                                        new Object[] { streamName, zkRootPath, cause });
                                            streamEntries.remove(streamName);
                                        } else {
                                            if (1 == numFailures.incrementAndGet()) {
                                                promise.setException(cause);
                                            }
                                        }
                                        complete();
                                    }

                                    private void complete() {
                                        if (0 == numPendings.decrementAndGet() && numFailures.get() == 0) {
                                            promise.setValue(null);
                                        }
                                    }
                                });
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        } catch (InterruptedException e) {
            promise.setException(e);
        }
    }

    private Future<ZKAccessControl> fetchDefaultAccessControlEntry() {
        final Promise<ZKAccessControl> promise = new Promise<ZKAccessControl>();
        fetchDefaultAccessControlEntry(promise);
        return promise;
    }

    private void fetchDefaultAccessControlEntry(final Promise<ZKAccessControl> promise) {
        ZKAccessControl.read(zkc, zkRootPath, this)
            .addEventListener(new FutureEventListener<ZKAccessControl>() {
                @Override
                public void onSuccess(ZKAccessControl accessControl) {
                    logger.info("Default Access Control will be changed from {} to {}",
                                ZKAccessControlManager.this.defaultAccessControl,
                                accessControl);
                    ZKAccessControlManager.this.defaultAccessControl = accessControl;
                    promise.setValue(accessControl);
                }

                @Override
                public void onFailure(Throwable cause) {
                    if (cause instanceof KeeperException.NoNodeException) {
                        logger.info("Default Access Control is missing, creating one for {} ...", zkRootPath);
                        createDefaultAccessControlEntryIfNeeded(promise);
                    } else {
                        promise.setException(cause);
                    }
                }
            });
    }

    private void createDefaultAccessControlEntryIfNeeded(final Promise<ZKAccessControl> promise) {
        ZooKeeper zk;
        try {
            zk = zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
            return;
        } catch (InterruptedException e) {
            promise.setException(e);
            return;
        }
        ZkUtils.createFullPathOptimistic(zk, zkRootPath, new byte[0], zkc.getDefaultACL(),
                CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (KeeperException.Code.OK.intValue() == rc) {
                    logger.info("Created zk path {} for default ACL.", zkRootPath);
                    fetchDefaultAccessControlEntry(promise);
                } else {
                    promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                }
            }
        }, null);
    }

    private void refetchDefaultAccessControlEntry(final int delayMs) {
        if (closed) {
            return;
        }
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                fetchDefaultAccessControlEntry().addEventListener(new FutureEventListener<ZKAccessControl>() {
                    @Override
                    public void onSuccess(ZKAccessControl value) {
                        // no-op
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        if (cause instanceof ZKAccessControl.CorruptedAccessControlException) {
                            logger.warn("Default access control entry is corrupted, ignore this update : ", cause);
                            return;
                        }

                        logger.warn("Encountered an error on refetching default access control entry, retrying in {} ms : ",
                                    ZK_RETRY_BACKOFF_MS, cause);
                        refetchDefaultAccessControlEntry(ZK_RETRY_BACKOFF_MS);
                    }
                });
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void refetchAccessControlEntries(final int delayMs) {
        if (closed) {
            return;
        }
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                fetchAccessControlEntries().addEventListener(new FutureEventListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        // no-op
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        logger.warn("Encountered an error on refetching access control entries, retrying in {} ms : ",
                                    ZK_RETRY_BACKOFF_MS, cause);
                        refetchAccessControlEntries(ZK_RETRY_BACKOFF_MS);
                    }
                });
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void refetchAllAccessControlEntries(final int delayMs) {
        if (closed) {
            return;
        }
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                fetchDefaultAccessControlEntry().addEventListener(new FutureEventListener<ZKAccessControl>() {
                    @Override
                    public void onSuccess(ZKAccessControl value) {
                        fetchAccessControlEntries().addEventListener(new FutureEventListener<Void>() {
                            @Override
                            public void onSuccess(Void value) {
                                // no-op
                            }

                            @Override
                            public void onFailure(Throwable cause) {
                                logger.warn("Encountered an error on fetching all access control entries, retrying in {} ms : ",
                                            ZK_RETRY_BACKOFF_MS, cause);
                                refetchAccessControlEntries(ZK_RETRY_BACKOFF_MS);
                            }
                        });
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        logger.warn("Encountered an error on refetching all access control entries, retrying in {} ms : ",
                                    ZK_RETRY_BACKOFF_MS, cause);
                        refetchAllAccessControlEntries(ZK_RETRY_BACKOFF_MS);
                    }
                });
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(WatchedEvent event) {
        if (Event.EventType.None.equals(event.getType())) {
            if (event.getState() == Event.KeeperState.Expired) {
                refetchAllAccessControlEntries(0);
            }
        } else if (Event.EventType.NodeDataChanged.equals(event.getType())) {
            logger.info("Default ACL for {} is changed, refetching ...", zkRootPath);
            refetchDefaultAccessControlEntry(0);
        } else if (Event.EventType.NodeChildrenChanged.equals(event.getType())) {
            logger.info("List of ACLs for {} are changed, refetching ...", zkRootPath);
            refetchAccessControlEntries(0);
        }
    }
}
