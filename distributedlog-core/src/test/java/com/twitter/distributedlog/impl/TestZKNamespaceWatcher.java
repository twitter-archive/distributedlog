package com.twitter.distributedlog.impl;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.ZooKeeperClientUtils;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.distributedlog.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Test ZK Namespace Watcher.
 */
public class TestZKNamespaceWatcher extends TestDistributedLogBase {

    private final static int zkSessionTimeoutMs = 2000;

    @Rule
    public TestName runtime = new TestName();
    protected final DistributedLogConfiguration baseConf =
            new DistributedLogConfiguration();
    protected ZooKeeperClient zkc;
    protected OrderedScheduler scheduler;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .uri(createDLMURI("/"))
                .zkAclId(null)
                .sessionTimeoutMs(zkSessionTimeoutMs)
                .build();
        scheduler = OrderedScheduler.newBuilder()
                .name("test-zk-namespace-watcher")
                .corePoolSize(1)
                .build();
    }

    @After
    public void teardown() throws Exception {
        if (null != zkc) {
            zkc.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    private void createLogInNamespace(URI uri, String logName) throws Exception {
        String logPath = uri.getPath() + "/" + logName;
        Utils.zkCreateFullPathOptimistic(zkc, logPath, new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void deleteLogInNamespace(URI uri, String logName) throws Exception {
        String logPath = uri.getPath() + "/" + logName;
        zkc.get().delete(logPath, -1);
    }

    @Test(timeout = 60000)
    public void testNamespaceListener() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        zkc.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.addConfiguration(baseConf);
        ZKNamespaceWatcher watcher = new ZKNamespaceWatcher(conf, uri, zkc, scheduler);
        final CountDownLatch[] latches = new CountDownLatch[10];
        for (int i = 0; i < 10; i++) {
            latches[i] = new CountDownLatch(1);
        }
        final AtomicInteger numUpdates = new AtomicInteger(0);
        final AtomicReference<Set<String>> receivedLogs = new AtomicReference<Set<String>>(null);
        watcher.registerListener(new NamespaceListener() {
            @Override
            public void onStreamsChanged(Iterator<String> streams) {
                Set<String> streamSet = Sets.newHashSet(streams);
                int updates = numUpdates.incrementAndGet();
                receivedLogs.set(streamSet);
                latches[updates - 1].countDown();
            }
        });
        // first update
        final Set<String> expectedLogs = Sets.newHashSet();
        latches[0].await();
        validateReceivedLogs(expectedLogs, receivedLogs.get());

        // create test1
        expectedLogs.add("test1");
        createLogInNamespace(uri, "test1");
        latches[1].await();
        validateReceivedLogs(expectedLogs, receivedLogs.get());

        // create invalid log
        createLogInNamespace(uri, ".test1");
        latches[2].await();
        validateReceivedLogs(expectedLogs, receivedLogs.get());

        // create test2
        expectedLogs.add("test2");
        createLogInNamespace(uri, "test2");
        latches[3].await();
        validateReceivedLogs(expectedLogs, receivedLogs.get());

        // delete test1
        expectedLogs.remove("test1");
        deleteLogInNamespace(uri, "test1");
        latches[4].await();
        validateReceivedLogs(expectedLogs, receivedLogs.get());
    }

    private void validateReceivedLogs(Set<String> expectedLogs, Set<String> receivedLogs) {
        assertTrue(Sets.difference(expectedLogs, receivedLogs).isEmpty());
    }

    @Test(timeout = 60000)
    public void testSessionExpired() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        zkc.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.addConfiguration(baseConf);
        ZKNamespaceWatcher watcher = new ZKNamespaceWatcher(conf, uri, zkc, scheduler);
        final CountDownLatch[] latches = new CountDownLatch[10];
        for (int i = 0; i < 10; i++) {
            latches[i] = new CountDownLatch(1);
        }
        final AtomicInteger numUpdates = new AtomicInteger(0);
        final AtomicReference<Set<String>> receivedLogs = new AtomicReference<Set<String>>(null);
        watcher.registerListener(new NamespaceListener() {
            @Override
            public void onStreamsChanged(Iterator<String> streams) {
                Set<String> streamSet = Sets.newHashSet(streams);
                int updates = numUpdates.incrementAndGet();
                receivedLogs.set(streamSet);
                latches[updates - 1].countDown();
            }
        });
        latches[0].await();
        createLogInNamespace(uri, "test1");
        latches[1].await();
        createLogInNamespace(uri, "test2");
        latches[2].await();
        assertEquals(2, receivedLogs.get().size());
        ZooKeeperClientUtils.expireSession(zkc, DLUtils.getZKServersFromDLUri(uri), zkSessionTimeoutMs);
        latches[3].await();
        assertEquals(2, receivedLogs.get().size());
        createLogInNamespace(uri, "test3");
        latches[4].await();
        assertEquals(3, receivedLogs.get().size());
    }

}
