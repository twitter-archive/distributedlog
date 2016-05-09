/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import com.google.common.base.Stopwatch;

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
 * Utilities of {@link com.twitter.distributedlog.ZooKeeperClient}
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

        boolean done = false;
        Stopwatch expireWait = Stopwatch.createStarted();
        while (!done && expireWait.elapsed(TimeUnit.MILLISECONDS) < timeout*2) {
            try {
                zkc.get().exists("/", false);
                done = true;
            } catch (KeeperException ke) {
                done = (ke.code() == KeeperException.Code.SESSIONEXPIRED);
            }
        }

        assertTrue("Client should receive session expired event.",
                   expireLatch.await(timeout, TimeUnit.MILLISECONDS));
    }
}
