package com.twitter.distributedlog;

import com.twitter.distributedlog.ZooKeeperClient.Credentials;
import com.twitter.distributedlog.ZooKeeperClient.DigestCredentials;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Test Cases for {@link com.twitter.distributedlog.ZooKeeperClient}
 */
public class TestZooKeeperClient extends ZooKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperClient.class);

    private final static int sessionTimeoutMs = 2000;

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = buildClient();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    private ZooKeeperClientBuilder clientBuilder() throws Exception {
        return ZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI(zkPort, "/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .zkServers(zkServers)
                .retryPolicy(new BoundExponentialBackoffRetryPolicy(100, 200, 2));
    }

    private ZooKeeperClient buildClient() throws Exception {
        return clientBuilder().zkAclId(null).build();
    }

    private ZooKeeperClient buildAuthdClient(String id) throws Exception {
        return clientBuilder().zkAclId(id).build();
    }

    private void rmAll(ZooKeeperClient client, String path) throws Exception {
        List<String> nodes = client.get().getChildren(path, false);
        for (String node : nodes) {
            String childPath = path + "/" + node;
            rmAll(client, childPath);
        }
        client.get().delete(path, 0);
    }

    @Test(timeout = 60000)
    public void testAclCreatePerms() throws Exception {
        ZooKeeperClient zkcAuth = buildAuthdClient("test");
        zkcAuth.get().create("/test", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkcAuth.get().create("/test/key1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkcAuth.get().create("/test/key2", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);

        ZooKeeperClient zkcNoAuth = buildClient();
        zkcNoAuth.get().create("/test/key1/key1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            zkcNoAuth.get().create("/test/key2/key1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            fail("create should fail on acl protected key");
        } catch (KeeperException.NoAuthException ex) {
            LOG.info("caught exception writing to protected key", ex);
        }

        rmAll(zkcAuth, "/test");
    }

    @Test(timeout = 60000)
    public void testAclNullIdDisablesAuth() throws Exception {
        ZooKeeperClient zkcAuth = buildAuthdClient(null);
        zkcAuth.get().create("/test", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkcAuth.get().create("/test/key1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            zkcAuth.get().create("/test/key2", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);
            fail("create should fail because we're not authenticated");
        } catch (KeeperException.InvalidACLException ex) {
            LOG.info("caught exception writing to protected key", ex);
        }

        rmAll(zkcAuth, "/test");
    }

    @Test(timeout = 60000)
    public void testAclAllowsReadsForNoAuth() throws Exception {
        ZooKeeperClient zkcAuth = buildAuthdClient("test");
        zkcAuth.get().create("/test", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);
        zkcAuth.get().create("/test/key1", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);
        zkcAuth.get().create("/test/key1/key2", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);

        ZooKeeperClient zkcNoAuth = buildClient();
        List<String> nodes = null;
        String path = "/test";
        nodes = zkcNoAuth.get().getChildren(path, false);
        path = path + "/" + nodes.get(0);
        nodes = zkcNoAuth.get().getChildren(path, false);
        assertEquals("key2", nodes.get(0));

        ZooKeeperClient zkcAuth2 = buildAuthdClient("test2");
        path = "/test";
        nodes = zkcNoAuth.get().getChildren(path, false);
        path = path + "/" + nodes.get(0);
        nodes = zkcNoAuth.get().getChildren(path, false);
        assertEquals("key2", nodes.get(0));

        rmAll(zkcAuth, "/test");
    }

    @Test(timeout = 60000)
    public void testAclDigestCredentialsBasics() throws Exception {
        ZooKeeperClient zkcAuth = buildClient();
        zkcAuth.get().create("/test", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        try {
            zkcAuth.get().create("/test/key1", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);
            fail("should have failed");
        } catch (Exception ex) {
        }

        Credentials credentials = new DigestCredentials("test", "test");
        credentials.authenticate(zkcAuth.get());

        // Should not throw now that we're authenticated.
        zkcAuth.get().create("/test/key1", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);

        rmAll(zkcAuth, "/test");
    }

    @Test(timeout = 60000)
    public void testAclNoopCredentialsDoesNothing() throws Exception {
        Credentials.NONE.authenticate(null);
    }

    class FailingCredentials implements Credentials {
        boolean shouldFail = true;
        @Override
        public void authenticate(ZooKeeper zooKeeper) {
            if (shouldFail) {
                throw new RuntimeException("authfailed");
            }
        }
        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }
    }

    @Test(timeout = 60000)
    public void testAclFailedAuthenticationCanBeRecovered() throws Exception {
        FailingCredentials credentials = new FailingCredentials();
        ZooKeeperClient zkc = new ZooKeeperClient("test", 2000, 2000, zkServers, null, null, 1, 10000, credentials);

        try {
            zkc.get();
            fail("should have failed on auth");
        } catch (Exception ex) {
            assertEquals("authfailed", ex.getMessage());
        }

        // Should recover fine
        credentials.setShouldFail(false);
        zkc.get().create("/test", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        rmAll(zkc, "/test");
    }

    private void expireZooKeeperSession(ZooKeeper zk, int timeout)
            throws IOException, InterruptedException, KeeperException {
        final CountDownLatch latch = new CountDownLatch(1);

        ZooKeeper newZk = new ZooKeeper(zkServers, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None && event.getState() == KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }},
            zk.getSessionId(),
            zk.getSessionPasswd());

        if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }

        newZk.close();
    }

    private CountDownLatch awaitConnectionEvent(final KeeperState state, final ZooKeeperClient zkc) {
        final CountDownLatch connected = new CountDownLatch(1);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None && event.getState() == state) {
                    connected.countDown();
                }
            }
        };
        zkc.register(watcher);
        return connected;
    }

    @Test(timeout = 60000)
    public void testAclAuthSpansExpiration() throws Exception {
        ZooKeeperClient zkcAuth = buildAuthdClient("test");
        zkcAuth.get().create("/test", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);

        CountDownLatch expired = awaitConnectionEvent(KeeperState.Expired, zkcAuth);
        CountDownLatch connected = awaitConnectionEvent(KeeperState.SyncConnected, zkcAuth);

        expireZooKeeperSession(zkcAuth.get(), 2000);

        expired.await(2, TimeUnit.SECONDS);
        connected.await(2, TimeUnit.SECONDS);

        zkcAuth.get().create("/test/key1", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);

        rmAll(zkcAuth, "/test");
    }

    @Test(timeout = 60000)
    public void testAclAuthSpansExpirationNonRetryableClient() throws Exception {
        ZooKeeperClient zkcAuth = clientBuilder().retryPolicy(null).zkAclId("test").build();
        zkcAuth.get().create("/test", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);

        CountDownLatch expired = awaitConnectionEvent(KeeperState.Expired, zkcAuth);
        CountDownLatch connected = awaitConnectionEvent(KeeperState.SyncConnected, zkcAuth);

        expireZooKeeperSession(zkcAuth.get(), 2000);

        expired.await(2, TimeUnit.SECONDS);
        connected.await(2, TimeUnit.SECONDS);

        zkcAuth.get().create("/test/key1", new byte[0], DistributedLogConstants.EVERYONE_READ_CREATOR_ALL, CreateMode.PERSISTENT);

        rmAll(zkcAuth, "/test");
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
