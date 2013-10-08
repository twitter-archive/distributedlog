package com.twitter.distributedlog;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


public class BookKeeperClient implements ZooKeeperClient.ZooKeeperSessionExpireNotifier {
    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClient.class);
    private int refCount;
    private BookKeeper bkc = null;
    private final ZooKeeperClient zkc;
    private final boolean ownZK;
    private final String name;
    private Watcher sessionExpireWatcher = null;
    private AtomicBoolean zkSessionExpired = new AtomicBoolean(false);

    private void commonInitialization(DistributedLogConfiguration conf, StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException {
        ClientConfiguration bkConfig = new ClientConfiguration();
        bkConfig.setAddEntryTimeout(conf.getBKClientWriteTimeout());
        bkConfig.setReadTimeout(conf.getBKClientReadTimeout());
        bkConfig.setZkLedgersRootPath(conf.getBKLedgersPath());
        bkConfig.setZkTimeout(conf.getZKSessionTimeoutSeconds() * 1000);
        this.bkc = new BookKeeper(bkConfig, zkc.get(), statsLogger);
        refCount = 1;
        sessionExpireWatcher = this.zkc.registerExpirationHandler(this);
    }

    public BookKeeperClient(DistributedLogConfiguration conf, String zkConnect, String name,
                            StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException {
        int zkSessionTimeout = conf.getZKSessionTimeoutSeconds() * 1000;
        this.zkc = new ZooKeeperClient(zkSessionTimeout, 2 * zkSessionTimeout, zkConnect);
        this.ownZK = true;
        this.name = name;
        commonInitialization(conf, statsLogger);
        LOG.info("BookKeeper Client created {} with its own ZK Client", name);
    }

    public BookKeeperClient(DistributedLogConfiguration conf, ZooKeeperClient zkc, String name,
                            StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException {
        this.zkc = zkc;
        this.ownZK = false;
        this.name = name;
        commonInitialization(conf, statsLogger);
        LOG.info("BookKeeper Client created {} with shared zookeeper client", name);
    }


    public synchronized BookKeeper get() throws IOException {
        checkClosedOrInError();
        return bkc;
    }

    public synchronized void addRef() {
        refCount++;
    }

    public synchronized void release() throws BKException, InterruptedException {
        refCount--;

        if (0 == refCount) {
            LOG.info("BookKeeper Client closed {}", name);
            bkc.close();
            bkc = null;
            zkc.unregister(sessionExpireWatcher);
            if (ownZK) {
                zkc.close();
            }
        }
    }

    @Override
    public void notifySessionExpired() {
        zkSessionExpired.set(true);
    }

    public synchronized void checkClosedOrInError() throws AlreadyClosedException {
        if (null == bkc) {
            LOG.error("BookKeeper Client is already closed");
            throw new AlreadyClosedException("BookKeeper Client is already closed");
        }

        if (zkSessionExpired.get()) {
            LOG.error("BookKeeper Client's Zookeeper session has expired");
            throw new AlreadyClosedException("BookKeeper Client's Zookeeper session has expired");
        }
    }
}
