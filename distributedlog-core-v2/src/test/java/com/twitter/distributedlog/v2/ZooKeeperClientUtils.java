package com.twitter.distributedlog.v2;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Utilities of {@link com.twitter.distributedlog.v2.ZooKeeperClient}
 */
public class ZooKeeperClientUtils {

    static final Logger logger = LoggerFactory.getLogger(ZooKeeperClientUtils.class);

    /**
     * Expire given zookeeper client's session.
     *
     * @param zkc
     *          zookeeper client
     * @param zkServers
     *          zookeeper servers
     * @param timeout
     *          timeout
     * @throws Exception
     */
    public static void expireSession(ZooKeeperClient zkc, String zkServers, int timeout)
            throws Exception {
        final CountDownLatch expireLatch = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper oldZk = zkc.get();
        oldZk.exists("/", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.debug("Receive event : {}", event);
                if (event.getType() == Event.EventType.None &&
                        event.getState() == Event.KeeperState.Expired) {
                    expireLatch.countDown();
                }
            }
        });
        ZooKeeper newZk = new ZooKeeper(zkServers, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (Event.EventType.None == event.getType() &&
                        Event.KeeperState.SyncConnected == event.getState()) {
                    latch.countDown();
                }
            }
        }, oldZk.getSessionId(), oldZk.getSessionPasswd());
        if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
        newZk.close();
        Thread.sleep(2 * timeout);
        try {
            zkc.get().exists("/", false);
        } catch (KeeperException ke) {
            // expected
        }
        assertTrue("Client should receive session expired event.",
                   expireLatch.await(timeout, TimeUnit.MILLISECONDS));
    }
}
