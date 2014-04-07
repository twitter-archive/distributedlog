package com.twitter.distributedlog;

import com.twitter.distributedlog.metadata.BKDLConfig;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


class BookKeeperClient implements ZooKeeperClient.ZooKeeperSessionExpireNotifier {
    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClient.class);
    private int refCount;
    private BookKeeper bkc = null;
    private final ZooKeeperClient zkc;
    private final boolean ownZK;
    private final String name;
    private Watcher sessionExpireWatcher = null;
    private AtomicBoolean zkSessionExpired = new AtomicBoolean(false);

    private synchronized void commonInitialization(
            DistributedLogConfiguration conf, BKDLConfig bkdlConfig,
            ClientSocketChannelFactory channelFactory, StatsLogger statsLogger, HashedWheelTimer requestTimer)
        throws IOException, InterruptedException, KeeperException {
        ClientConfiguration bkConfig = new ClientConfiguration();
        bkConfig.setAddEntryTimeout(conf.getBKClientWriteTimeout());
        bkConfig.setReadTimeout(conf.getBKClientReadTimeout());
        bkConfig.setZkLedgersRootPath(bkdlConfig.getBkLedgersPath());
        bkConfig.setZkTimeout(conf.getBKClientZKSessionTimeoutMilliSeconds());
        bkConfig.setNumWorkerThreads(conf.getBKClientNumberWorkerThreads());
        this.bkc = BookKeeper.newBuilder()
            .config(bkConfig)
            .zk(zkc.get())
            .channelFactory(channelFactory)
            .statsLogger(statsLogger)
            .channelFactory(channelFactory)
            .requestTimer(requestTimer).build();

        refCount = 1;
        sessionExpireWatcher = this.zkc.registerExpirationHandler(this);
    }

    BookKeeperClient(DistributedLogConfiguration conf, BKDLConfig bkdlConfig, ZooKeeperClient zkc,
                     String name, ClientSocketChannelFactory channelFactory, HashedWheelTimer requestTimer, StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException {
        if (null == zkc) {
            int zkSessionTimeout = conf.getBKClientZKSessionTimeoutMilliSeconds();
            this.zkc = new ZooKeeperClient(zkSessionTimeout, 2 * zkSessionTimeout, bkdlConfig.getZkServers());
            this.ownZK = true;
        } else {
            this.zkc = zkc;
            this.ownZK = false;
        }

        this.name = name;
        commonInitialization(conf, bkdlConfig, channelFactory, statsLogger, requestTimer);
        LOG.info("BookKeeper Client created {} with {} zookeeper client", name, ownZK ? "its own": "shared");
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
