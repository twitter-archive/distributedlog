package com.twitter.distributedlog;

import com.twitter.conversions.time;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Test Cases for {@link com.twitter.distributedlog.ZooKeeperClient}
 */
public class TestZooKeeperClient {

    private static ZooKeeperServerShim zks;
    private static String zkServers;
    private final static int sessionTimeoutMs = 2000;

    private ZooKeeperClient zkc;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        zkServers = "127.0.0.1:7000";
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI("/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    static class TestWatcher implements Watcher {

        final List<WatchedEvent> receivedEvents = new ArrayList<WatchedEvent>();
        CountDownLatch latch = new CountDownLatch(0);

        public TestWatcher setLatch(CountDownLatch latch) {
            this.latch = latch;
            return this;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                synchronized (receivedEvents) {
                    receivedEvents.add(event);
                }
                latch.countDown();
            }
        }
    }

    @Test(timeout = 60000)
    public void testRegisterUnregisterWatchers() throws Exception {
        TestWatcher w1 = new TestWatcher();
        TestWatcher w2 = new TestWatcher();

        final CountDownLatch latch = new CountDownLatch(2);
        w1.setLatch(latch);
        w2.setLatch(latch);

        zkc.register(w1);
        zkc.register(w2);

        assertEquals(2, zkc.watchers.size());

        final String zkPath = "/test-register-unregister-watchers";

        zkc.get().create(zkPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.get().getData(zkPath, true, new Stat());

        zkc.get().setData(zkPath, "first-set".getBytes(), -1);
        latch.await();
        assertEquals(1, w1.receivedEvents.size());
        assertEquals(zkPath, w1.receivedEvents.get(0).getPath());
        assertEquals(Watcher.Event.EventType.NodeDataChanged, w1.receivedEvents.get(0).getType());
        assertEquals(1, w2.receivedEvents.size());
        assertEquals(zkPath, w2.receivedEvents.get(0).getPath());
        assertEquals(Watcher.Event.EventType.NodeDataChanged, w2.receivedEvents.get(0).getType());

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        w1.setLatch(latch1);
        w2.setLatch(latch2);

        zkc.unregister(w2);

        assertEquals(1, zkc.watchers.size());
        zkc.get().getData(zkPath, true, new Stat());
        zkc.get().setData(zkPath, "second-set".getBytes(), -1);
        latch1.await();
        assertEquals(2, w1.receivedEvents.size());
        assertEquals(zkPath, w1.receivedEvents.get(1).getPath());
        assertEquals(Watcher.Event.EventType.NodeDataChanged, w1.receivedEvents.get(1).getType());
        assertFalse(latch2.await(2, TimeUnit.SECONDS));
        assertEquals(1, w2.receivedEvents.size());
    }


    @Test(timeout = 60000)
    public void testExceptionOnWatchers() throws Exception {
        TestWatcher w1 = new TestWatcher();
        TestWatcher w2 = new TestWatcher();

        final CountDownLatch latch = new CountDownLatch(2);
        w1.setLatch(latch);
        w2.setLatch(latch);

        zkc.register(w1);
        zkc.register(w2);
        // register bad watcher
        zkc.register(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                throw new NullPointerException("bad watcher returning null");
            }
        });

        assertEquals(3, zkc.watchers.size());

        final String zkPath = "/test-exception-on-watchers";

        zkc.get().create(zkPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.get().getData(zkPath, true, new Stat());

        zkc.get().setData(zkPath, "first-set".getBytes(), -1);
        latch.await();
        assertEquals(1, w1.receivedEvents.size());
        assertEquals(zkPath, w1.receivedEvents.get(0).getPath());
        assertEquals(Watcher.Event.EventType.NodeDataChanged, w1.receivedEvents.get(0).getType());
        assertEquals(1, w2.receivedEvents.size());
        assertEquals(zkPath, w2.receivedEvents.get(0).getPath());
        assertEquals(Watcher.Event.EventType.NodeDataChanged, w2.receivedEvents.get(0).getType());
    }
}
