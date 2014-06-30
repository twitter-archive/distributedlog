package com.twitter.distributedlog;

import com.twitter.distributedlog.net.TwitterDNSResolver;
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
import org.jboss.netty.util.HashedWheelTimer;
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
            DistributedLogConfiguration conf, String ledgersPath,
            ClientSocketChannelFactory channelFactory, StatsLogger statsLogger, HashedWheelTimer requestTimer,
            boolean registerExpirationHandler)
        throws IOException, InterruptedException, KeeperException {
        ClientConfiguration bkConfig = new ClientConfiguration();
        bkConfig.setAddEntryTimeout(conf.getBKClientWriteTimeout());
        bkConfig.setReadTimeout(conf.getBKClientReadTimeout());
        bkConfig.setZkLedgersRootPath(ledgersPath);
        bkConfig.setZkTimeout(conf.getBKClientZKSessionTimeoutMilliSeconds());
        bkConfig.setNumWorkerThreads(conf.getBKClientNumberWorkerThreads());
        bkConfig.setEnsemblePlacementPolicy(RegionAwareEnsemblePlacementPolicy.class);
        bkConfig.setZkRequestRateLimit(conf.getBKClientZKRequestRateLimit());
        // reload configuration from dl configuration with settings prefixed with 'bkc.'
        ConfUtils.loadConfiguration(bkConfig, conf, "bkc.");
        this.bkc = BookKeeper.newBuilder()
            .config(bkConfig)
            .zk(zkc.get())
            .channelFactory(channelFactory)
            .statsLogger(statsLogger)
            .dnsResolver(new TwitterDNSResolver(conf.getBkDNSResolverOverrides()))
            .requestTimer(requestTimer).build();

        refCount = 1;
        if (registerExpirationHandler) {
            sessionExpireWatcher = this.zkc.registerExpirationHandler(this);
        }
    }

    BookKeeperClient(DistributedLogConfiguration conf,
                     String name,
                     String zkServers,
                     ZooKeeperClient zkc,
                     String ledgersPath,
                     ClientSocketChannelFactory channelFactory,
                     HashedWheelTimer requestTimer,
                     StatsLogger statsLogger)
        throws IOException, InterruptedException, KeeperException {
        boolean registerExpirationHandler;
        if (null == zkc) {
            int zkSessionTimeout = conf.getBKClientZKSessionTimeoutMilliSeconds();
            RetryPolicy retryPolicy = null;
            if (conf.getBKClientZKNumRetries() > 0) {
                retryPolicy = new BoundExponentialBackoffRetryPolicy(
                        conf.getBKClientZKRetryBackoffStartMillis(),
                        conf.getBKClientZKRetryBackoffMaxMillis(), conf.getBKClientZKNumRetries());
            }
            this.zkc = new ZooKeeperClient(name + ":zk", zkSessionTimeout, 2 * zkSessionTimeout, zkServers,
                                           retryPolicy, statsLogger.scope("bkc_zkc"), conf.getZKClientNumberRetryThreads(),
                                           conf.getBKClientZKRequestRateLimit());
            this.ownZK = true;
            registerExpirationHandler = conf.getBKClientZKNumRetries() <= 0;
        } else {
            this.zkc = zkc;
            this.ownZK = false;
            registerExpirationHandler = conf.getZKNumRetries() <= 0;
        }

        this.name = name;
        commonInitialization(conf, ledgersPath, channelFactory, statsLogger, requestTimer, registerExpirationHandler);

        if (ownZK) {
            LOG.info("BookKeeper Client created {} with its own ZK Client : ledgersPath = {}, numRetries = {}, " +
                    "sessionTimeout = {}, backoff = {}, maxBackoff = {}, dnsResolver = {}, registerExpirationHandler = {}",
                    new Object[] { name, ledgersPath,
                    conf.getBKClientZKNumRetries(), conf.getBKClientZKSessionTimeoutMilliSeconds(),
                    conf.getBKClientZKRetryBackoffStartMillis(), conf.getBKClientZKRetryBackoffMaxMillis(),
                    conf.getBkDNSResolverOverrides(), registerExpirationHandler });
        } else {
            LOG.info("BookKeeper Client created {} with shared zookeeper client : ledgersPath = {}, numRetries = {}, " +
                    "sessionTimeout = {}, backoff = {}, maxBackoff = {}, dnsResolver = {}, registerExpirationHandler = {}",
                    new Object[] { name, ledgersPath,
                    conf.getZKNumRetries(), conf.getZKSessionTimeoutMilliseconds(),
                    conf.getZKRetryBackoffStartMillis(), conf.getZKRetryBackoffMaxMillis(),
                    conf.getBkDNSResolverOverrides(), registerExpirationHandler });
        }
    }


    public synchronized BookKeeper get() throws IOException {
        checkClosedOrInError();
        return bkc;
    }

    public synchronized void addRef() {
        refCount++;
    }

    public void release() {
        release(false);
    }

    /**
     * TODO: force close is a temp solution. we need to figure out ref count leaking.
     * {@link https://jira.twitter.biz/browse/PUBSUB-2232}
     */
    public synchronized void release(boolean force) {
        refCount--;

        if (0 == refCount || force) {
            LOG.info("BookKeeper Client closed {} : refs = {}, force = {}",
                     new Object[] { name, refCount, force });
            if (null != bkc) {
                try {
                    bkc.close();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted on closing bookkeeper client {} : ", name, e);
                    Thread.currentThread().interrupt();
                } catch (BKException e) {
                    LOG.warn("Error on closing bookkeeper client {} : ", name, e);
                }
                bkc = null;
            }
            if (null != sessionExpireWatcher) {
                zkc.unregister(sessionExpireWatcher);
            }
            if (ownZK) {
                zkc.close(force);
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
