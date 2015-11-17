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
package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.v2.metadata.BKDLConfig;
import com.twitter.distributedlog.v2.metadata.DLMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for setting up bookkeeper ensembles
 * and bringing individual bookies up and down
 */
public class LocalDLMEmulator {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDLMEmulator.class);
    private static final int DEFAULT_BOOKIE_INITIAL_PORT = 0; // Use ephemeral ports
    private static final int DEFAULT_ZK_TIMEOUT_SEC = 10;
    private static final int DEFAULT_ZK_PORT = 2181;
    private static final String DEFAULT_ZK_HOST = "127.0.0.1";
    private static final String DEFAULT_ZK_ENSEMBLE = DEFAULT_ZK_HOST + ":" + DEFAULT_ZK_PORT;
    private static final int DEFAULT_NUM_BOOKIES = 3;
    private static final String DLOG_NAMESPACE = "/messaging/distributedlog";

    private final Thread bkStartupThread;
    private final String zkHost;
    private final int zkPort;
    private final int zkTimeoutSec;
    private final int numBookies;

    public static class Builder {
        private int zkTimeoutSec = DEFAULT_ZK_TIMEOUT_SEC;
        private int numBookies = DEFAULT_NUM_BOOKIES;
        private String zkHost = DEFAULT_ZK_HOST;
        private int zkPort = DEFAULT_ZK_PORT;
        private int initialBookiePort = DEFAULT_BOOKIE_INITIAL_PORT;
        private boolean shouldStartZK = true;

        public Builder numBookies(int numBookies) {
            this.numBookies = numBookies;
            return this;
        }
        public Builder zkHost(String zkHost) {
            this.zkHost = zkHost;
            return this;
        }
        public Builder zkPort(int zkPort) {
            this.zkPort = zkPort;
            return this;
        }
        public Builder zkTimeoutSec(int zkTimeoutSec) {
            this.zkTimeoutSec = zkTimeoutSec;
            return this;
        }
        public Builder initialBookiePort(int initialBookiePort) {
            this.initialBookiePort = initialBookiePort;
            return this;
        }
        public Builder shouldStartZK(boolean shouldStartZK) {
            this.shouldStartZK = shouldStartZK;
            return this;
        }

        public LocalDLMEmulator build() throws Exception {
            return new LocalDLMEmulator(numBookies, shouldStartZK, zkHost, zkPort, initialBookiePort, zkTimeoutSec);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public LocalDLMEmulator(final int numBookies) throws Exception {
        this(numBookies, true, DEFAULT_ZK_HOST, DEFAULT_ZK_PORT, DEFAULT_BOOKIE_INITIAL_PORT, DEFAULT_ZK_TIMEOUT_SEC);
    }

    public LocalDLMEmulator(final int numBookies, final String zkHost, final int zkPort) throws Exception {
        this(numBookies, false, zkHost, zkPort, DEFAULT_BOOKIE_INITIAL_PORT, DEFAULT_ZK_TIMEOUT_SEC);
    }

    public LocalDLMEmulator(final int numBookies, final int initialBookiePort) throws Exception {
        this(numBookies, true, DEFAULT_ZK_HOST, DEFAULT_ZK_PORT, initialBookiePort, DEFAULT_ZK_TIMEOUT_SEC);
    }

    public LocalDLMEmulator(final int numBookies, final String zkHost, final int zkPort, final int initialBookiePort) throws Exception {
        this(numBookies, false, zkHost, zkPort, initialBookiePort, DEFAULT_ZK_TIMEOUT_SEC);
    }

    private LocalDLMEmulator(final int numBookies, final boolean shouldStartZK, final String zkHost, final int zkPort, final int initialBookiePort, final int zkTimeoutSec) throws Exception {
        this.numBookies = numBookies;
        this.zkHost = zkHost;
        this.zkPort = zkPort;
        this.zkTimeoutSec = zkTimeoutSec;

        final ServerConfiguration bkConfiguration = new ServerConfiguration();
        bkConfiguration.setZkTimeout(zkTimeoutSec * 1000);

        this.bkStartupThread = new Thread() {
            public void run() {
                try {
                    LocalBookKeeper.startLocalBookies(zkHost, zkPort, numBookies, shouldStartZK, initialBookiePort, bkConfiguration);
                } catch (InterruptedException e) {
                    // go away quietly
                } catch (Exception e) {
                    LOG.error("Error starting local bk", e);
                }
            }
        };
    }

    private String getZkEnsemble() {
        return zkHost + ":" + zkPort;
    }

    public void start() throws Exception {
        bkStartupThread.start();
        if (!LocalBookKeeper.waitForServerUp(getZkEnsemble(), zkTimeoutSec)) {
            throw new Exception("Error starting zookeeper/bookkeeper");
        }
        int bookiesUp = checkBookiesUp(numBookies, zkTimeoutSec);
        assert (numBookies == bookiesUp);

        // Provision the DistributedLog Namespace
        URI uri = URI.create("distributedlog://" + getZkEnsemble() + DLOG_NAMESPACE);
        DLMetadata.create(new BKDLConfig(getZkEnsemble(), "/ledgers")).create(uri);
    }

    public void teardown() throws Exception {
        if (bkStartupThread != null) {
            bkStartupThread.interrupt();
            bkStartupThread.join();
        }
    }

    public static ZooKeeper connectZooKeeper(String zkHost, int zkPort)
        throws IOException, KeeperException, InterruptedException {
        return connectZooKeeper(zkHost, zkPort, DEFAULT_ZK_TIMEOUT_SEC);
    }

    public static ZooKeeper connectZooKeeper(String zkHost, int zkPort, int zkTimeoutSec)
        throws IOException, KeeperException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String zkHostPort = zkHost + ":" + zkPort;
        ZooKeeper zkc = new ZooKeeper(zkHostPort, DEFAULT_ZK_TIMEOUT_SEC * 1000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        });
        if (!latch.await(zkTimeoutSec, TimeUnit.SECONDS)) {
            throw new IOException("Zookeeper took too long to connect");
        }
        return zkc;
    }

    public static URI createDLMURI(String path) throws Exception {
        return createDLMURI(DEFAULT_ZK_ENSEMBLE, path);
    }

    public static URI createDLMURI(String zkServers, String path) throws Exception {
        return URI.create("distributedlog://" + zkServers + DLOG_NAMESPACE + path);
    }

    public BookieServer newBookie() throws Exception {
        ServerConfiguration bookieConf = new ServerConfiguration();
        bookieConf.setZkTimeout(zkTimeoutSec * 1000);
        bookieConf.setBookiePort(0);
        String bookieDirPrefix = "bookie_" + UUID.randomUUID() + "_";
        File tmpdir = File.createTempFile(bookieDirPrefix, "test");
        if (!tmpdir.delete()) {
            LOG.debug("Fail to delete tmpdir " + tmpdir);
        }
        if (!tmpdir.mkdir()) {
            throw new IOException("Fail to create tmpdir " + tmpdir);
        }

        bookieConf.setZkServers(getZkEnsemble());
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
        ZooKeeper zkc = connectZooKeeper(zkHost, zkPort, zkTimeoutSec);
        try {
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

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: LocalDLEmulator <zk_port>");
            System.exit(-1);
        }
        int zkPort = Integer.parseInt(args[0]);

        final LocalDLMEmulator localDlm = LocalDLMEmulator.newBuilder()
            .zkPort(zkPort)
            .build();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    localDlm.teardown();
                    System.out.println("ByeBye!");
                } catch (Exception e) {
                    // do nothing
                }
            }
        });
        localDlm.start();
        System.out.println(String.format(
            "DistributedLog Sandbox is running now. You could access distributedlog://%s:%s",
            DEFAULT_ZK_HOST,
            zkPort));
    }
}
