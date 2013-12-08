package com.twitter.distributedlog;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;

public class Utils {
    private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

    /**
     * Current time from some arbitrary time base in the past, counting in
     * nanoseconds, and not affected by settimeofday or similar system clock
     * changes. This is appropriate to use when computing how much longer to
     * wait for an interval to expire.
     *
     * @return current time in nanoseconds.
     */
    public static long nowInNanos() {
        return System.nanoTime();
    }

    /**
     * Current time from some fixed base time - so useful for cross machine
     * comparison
     *
     * @return current time in milliseconds.
     */
    public static long nowInMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Milliseconds elapsed since the time specified, the input is nanoTime
     * the only conversion happens when computing the elapsed time
     *
     * @param startNanoTime the start of the interval that we are measuring
     * @return elapsed time in milliseconds.
     */
    public static long elapsedMSec(long startMsecTime) {
        return (System.currentTimeMillis() - startMsecTime);
    }

    public static boolean randomPercent(int percent) {
        return (Math.random() * 100) <= percent;
    }

    public static void zkCreateFullPathOptimistic(
        ZooKeeperClient zkc,
        String path,
        byte[] data,
        final List<ACL> acl,
        final CreateMode createMode)
        throws ZooKeeperClient.ZooKeeperConnectionException, KeeperException, InterruptedException {
        try {
            zkc.get().create(path, data, acl, createMode);
        } catch (KeeperException.NoNodeException nne) {
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash <= 0) {
                throw nne;
            }
            String parent = path.substring(0, lastSlash);
            zkCreateFullPathOptimistic(zkc, parent, new byte[0], acl, createMode);
            zkc.get().create(path, data, acl, createMode);
        }
    }

    /**
     * Simple watcher to notify when zookeeper has connected
     */
    public static class ZkConnectionWatcher implements Watcher {
        CountDownLatch zkConnectLatch;

        public ZkConnectionWatcher(CountDownLatch zkConnectLatch) {
            this.zkConnectLatch = zkConnectLatch;
        }

        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                zkConnectLatch.countDown();
            }
        }
    }
}
