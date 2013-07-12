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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for setting up bookkeeper ensembles
 * and bringing individual bookies up and down
 */
public class LocalDLMEmulator {
    protected static final Logger LOG = LoggerFactory.getLogger(LocalDLMEmulator.class);
    protected static final int DEFAULT_BOOKIE_INITIAL_PORT = 0; // Use ephemeral ports

    int nextPort = 6000; // next port for additionally created bookies
    private Thread bkthread = null;
    private final String zkEnsemble;
    int numBookies;
    private int initialBookiePort;

    public LocalDLMEmulator(final int numBookies) throws Exception {
        this(numBookies, true, "127.0.0.1", 2181, DEFAULT_BOOKIE_INITIAL_PORT);
    }

    public LocalDLMEmulator(final int numBookies, final String zkHost, final int zkPort) throws Exception {
        this(numBookies, false, zkHost, zkPort, DEFAULT_BOOKIE_INITIAL_PORT);
    }

    public LocalDLMEmulator(final int numBookies, final int initialBookiePort) throws Exception {
        this(numBookies, true, "127.0.0.1", 2181, initialBookiePort);
    }

    public LocalDLMEmulator(final int numBookies, final String zkHost, final int zkPort, final int initialBookiePort) throws Exception {
        this(numBookies, false, zkHost, zkPort, initialBookiePort);
    }

    private LocalDLMEmulator(final int numBookies, final boolean shouldStartZK, final String zkHost, final int zkPort, final int initialBookiePort) throws Exception {
        this.initialBookiePort = initialBookiePort;
        this.numBookies = numBookies;
        this.zkEnsemble = zkHost + ":" + zkPort;

        bkthread = new Thread() {
            public void run() {
                try {
                    LocalBookKeeper.startLocalBookie(zkHost, zkPort, numBookies, shouldStartZK, initialBookiePort);
                } catch (InterruptedException e) {
                    // go away quietly
                } catch (Exception e) {
                    LOG.error("Error starting local bk", e);
                }
            }
        };
    }

    public void start() throws Exception {
        bkthread.start();
        if (!LocalBookKeeper.waitForServerUp(zkEnsemble, 10000)) {
            throw new Exception("Error starting zookeeper/bookkeeper");
        }
        assert (numBookies == checkBookiesUp(numBookies, 10));
    }

    public void teardown() throws Exception {
        if (bkthread != null) {
            bkthread.interrupt();
            bkthread.join();
        }
    }

    public static ZooKeeper connectZooKeeper()
        throws IOException, KeeperException, InterruptedException{
        return connectZooKeeper("127.0.0.1", 2181);
    }

    public static ZooKeeper connectZooKeeper(String zkHost, int zkPort)
        throws IOException, KeeperException, InterruptedException {
            return connectZooKeeper(String.format("%s:%d", zkHost, zkPort));
    }

    public static ZooKeeper connectZooKeeper(String zkHostPort)
        throws IOException, KeeperException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        ZooKeeper zkc = new ZooKeeper(zkHostPort, 3600, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        });
        if (!latch.await(3, TimeUnit.SECONDS)) {
            throw new IOException("Zookeeper took too long to connect");
        }
        return zkc;
    }

    public static URI createDLMURI(String path) throws Exception {
        return createDLMURI("127.0.0.1:2181", path);
    }

    public static URI createDLMURI(String zkServers, String path) throws Exception {
        return URI.create("distributedlog://" + zkServers + path);
    }


    public BookieServer newBookie() throws Exception {
        int port = nextPort++;
        ServerConfiguration bookieConf = new ServerConfiguration();
        bookieConf.setBookiePort(port);
        File tmpdir = File.createTempFile("bookie" + Integer.toString(port) + "_",
            "test");
        tmpdir.delete();
        tmpdir.mkdir();

        bookieConf.setZkServers(zkEnsemble);
        bookieConf.setJournalDirName(tmpdir.getPath());
        bookieConf.setLedgerDirNames(new String[]{tmpdir.getPath()});

        BookieServer b = new BookieServer(bookieConf);
        b.start();
        for (int i = 0; i < 10 && !b.isRunning(); i++) {
            Thread.sleep(10000);
        }
        if (!b.isRunning()) {
            throw new IOException("Bookie would not start");
        }
        return b;
    }

    /**
     * Check that a number of bookies are available
     *
     * @param count number of bookies required
     * @param timeout number of seconds to wait for bookies to start
     * @throws java.io.IOException if bookies are not started by the time the timeout hits
     */
    public int checkBookiesUp(int count, int timeout) throws Exception {
        ZooKeeper zkc = connectZooKeeper(zkEnsemble);
        try {
            boolean up = false;
            int mostRecentSize = 0;
            for (int i = 0; i < timeout; i++) {
                try {
                    List<String> children = zkc.getChildren("/ledgers/available",
                        false);
                    children.remove("readonly");
                    mostRecentSize = children.size();
                    if ((mostRecentSize > count) || LOG.isDebugEnabled()) {
                        LOG.info("Found " + mostRecentSize + " bookies up, "
                            + "waiting for " + count);
                        if ((mostRecentSize > count) || LOG.isTraceEnabled()) {
                            for (String child : children) {
                                LOG.info(" server: " + child);
                            }
                        }
                    }
                    if (mostRecentSize == count) {
                        up = true;
                        break;
                    }
                } catch (KeeperException e) {
                    // ignore
                }
                Thread.sleep(1000);
            }
            return mostRecentSize;
        } finally {
            zkc.close();
        }
    }
}
