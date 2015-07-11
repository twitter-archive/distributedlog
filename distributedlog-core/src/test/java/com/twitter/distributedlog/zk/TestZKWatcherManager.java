package com.twitter.distributedlog.zk;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

public class TestZKWatcherManager {

    @Test(timeout = 60000)
    public void testRegisterUnregisterWatcher() throws Exception {
        ZKWatcherManager watcherManager = ZKWatcherManager.newBuilder()
                .name("test-register-unregister-watcher")
                .statsLogger(NullStatsLogger.INSTANCE)
                .build();
        String path = "/test-register-unregister-watcher";
        final List<WatchedEvent> events = new LinkedList<WatchedEvent>();
        final CountDownLatch latch = new CountDownLatch(2);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                events.add(event);
                latch.countDown();
            }
        };
        watcherManager.registerChildWatcher(path, watcher);

        // fire the event
        WatchedEvent event0 = new WatchedEvent(
                Watcher.Event.EventType.NodeCreated,
                Watcher.Event.KeeperState.SyncConnected,
                path);
        WatchedEvent event1 = new WatchedEvent(
                Watcher.Event.EventType.None,
                Watcher.Event.KeeperState.SyncConnected,
                path);
        WatchedEvent event2 = new WatchedEvent(
                Watcher.Event.EventType.NodeChildrenChanged,
                Watcher.Event.KeeperState.SyncConnected,
                path);
        watcher.process(event1);
        watcher.process(event2);

        latch.await();

        assertEquals(2, events.size());
        assertEquals(event1, events.get(0));
        assertEquals(event2, events.get(1));

        // unregister watcher
        watcherManager.unregisterChildWatcher(path, watcher);

        assertEquals(0, watcherManager.allWatches.size());
        assertEquals(0, watcherManager.childWatches.size());
    }
}
