package com.twitter.distributedlog;

import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.util.ConfUtils;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
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

    private synchronized void commonInitialization(
            DistributedLogConfiguration conf, BKDLConfig bkdlConfig,
            ClientSocketChannelFactory channelFactory, StatsLogger statsLogger,
            boolean registerExpirationHandler)
        throws IOException, InterruptedException, KeeperException {
        ClientConfiguration bkConfig = new ClientConfiguration();
        bkConfig.setAddEntryTimeout(conf.getBKClientWriteTimeout());
        bkConfig.setReadTimeout(conf.getBKClientReadTimeout());
        bkConfig.setZkLedgersRootPath(bkdlConfig.getBkLedgersPath());
        bkConfig.setZkTimeout(conf.getBKClientZKSessionTimeoutMilliSeconds());
        bkConfig.setNumWorkerThreads(conf.getBKClientNumberWorkerThreads());
        bkConfig.setEnsemblePlacementPolicy(RegionAwareEnsemblePlacementPolicy.class);
        bkConfig.setProperty("reppDnsResolverClass", conf.getBkDNSResolverClass());
        // reload configuration from dl configuration with settings prefixed with 'bkc.'
        ConfUtils.loadConfiguration(bkConfig, conf, "bkc.");
        if (null == channelFactory) {
            this.bkc = new BookKeeper(bkConfig, zkc.get(), statsLogger);
        } else {
            this.bkc = new BookKeeper(bkConfig, zkc.get(), channelFactory, statsLogger);
        }
        refCount = 1;
        if (registerExpirationHandler) {
            sessionExpireWatcher = this.zkc.registerExpirationHandler(this);
        }
    }

    BookKeeperClient(DistributedLogConfiguration conf, BKDLConfig bkdlConfig, String name,
                     ClientSocketChannelFactory channelFactory, StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException {
        int zkSessionTimeout = conf.getBKClientZKSessionTimeoutMilliSeconds();
        RetryPolicy retryPolicy = null;
        if (conf.getBKClientZKNumRetries() > 0) {
            retryPolicy = new BoundExponentialBackoffRetryPolicy(
                    conf.getBKClientZKRetryBackoffStartMillis(),
                    conf.getBKClientZKRetryBackoffMaxMillis(), conf.getBKClientZKNumRetries());
        }
        this.zkc = new ZooKeeperClient(zkSessionTimeout, 2 * zkSessionTimeout, bkdlConfig.getZkServers(),
                retryPolicy, statsLogger.scope("bkc_zkc"));
        this.ownZK = true;
        this.name = name;
        commonInitialization(conf, bkdlConfig, channelFactory, statsLogger, conf.getBKClientZKNumRetries() <= 0);
        LOG.info("BookKeeper Client created {} with its own ZK Client : numRetries = {}, " +
                " sessionTimeout = {}, backoff = {}, maxBackoff = {}, dnsResolver = {}", new Object[] { name,
                conf.getBKClientZKNumRetries(), zkSessionTimeout, conf.getBKClientZKRetryBackoffStartMillis(),
                conf.getBKClientZKRetryBackoffMaxMillis(), conf.getBkDNSResolverClass() });
    }

    BookKeeperClient(DistributedLogConfiguration conf, BKDLConfig bkdlConfig, ZooKeeperClient zkc,
                     String name, ClientSocketChannelFactory channelFactory, StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException {
        this.zkc = zkc;
        this.ownZK = false;
        this.name = name;
        commonInitialization(conf, bkdlConfig, channelFactory, statsLogger, true);
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
            if (null != sessionExpireWatcher) {
                zkc.unregister(sessionExpireWatcher);
            }
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
