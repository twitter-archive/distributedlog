package com.twitter.distributedlog;

import com.twitter.distributedlog.ZooKeeperClient.Credentials;
import com.twitter.distributedlog.ZooKeeperClient.DigestCredentials;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.net.TwitterDNSResolverForRacks;
import com.twitter.distributedlog.net.TwitterDNSResolverForRows;
import com.twitter.distributedlog.util.ConfUtils;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
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

import com.google.common.base.Optional;

public class BookKeeperClient implements ZooKeeperClient.ZooKeeperSessionExpireNotifier {
    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClient.class);

    // Parameters to build bookkeeper client
    private final DistributedLogConfiguration conf;
    private final String name;
    private final String zkServers;
    private final String ledgersPath;
    private final ClientSocketChannelFactory channelFactory;
    private final HashedWheelTimer requestTimer;
    private final StatsLogger statsLogger;

    // bookkeeper client state
    private boolean closed = false;
    private BookKeeper bkc = null;
    private ZooKeeperClient zkc;
    private final boolean ownZK;
    // feature provider
    private final Optional<FeatureProvider> featureProvider;

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
        bkConfig.setProperty(RegionAwareEnsemblePlacementPolicy.REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME,
                DistributedLogConstants.DISALLOW_PLACEMENT_IN_REGION_FEATURE_NAME);
        // reload configuration from dl configuration with settings prefixed with 'bkc.'
        ConfUtils.loadConfiguration(bkConfig, conf, "bkc.");
        final DNSToSwitchMapping dnsResolver;
        if (conf.getRowAwareEnsemblePlacementEnabled()) {
            dnsResolver = new TwitterDNSResolverForRows(conf.getBkDNSResolverOverrides());
        } else {
            dnsResolver = new TwitterDNSResolverForRacks(conf.getBkDNSResolverOverrides());
        }

        this.bkc = BookKeeper.newBuilder()
            .config(bkConfig)
            .zk(zkc.get())
            .channelFactory(channelFactory)
            .statsLogger(statsLogger)
            .dnsResolver(dnsResolver)
            .requestTimer(requestTimer)
            .featureProvider(featureProvider.orNull())
            .build();

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
                     StatsLogger statsLogger,
                     Optional<FeatureProvider> featureProvider) {
        this.conf = conf;
        this.name = name;
        this.zkServers = zkServers;
        this.ledgersPath = ledgersPath;
        this.channelFactory = channelFactory;
        this.requestTimer = requestTimer;
        this.statsLogger = statsLogger;
        this.featureProvider = featureProvider;
        this.ownZK = null == zkc;
        if (null != zkc) {
            // reference the passing zookeeper client
            this.zkc = zkc;
        }
    }

    private synchronized void initialize() throws IOException {
        if (null != this.bkc) {
            return;
        }
        boolean registerExpirationHandler;
        if (null == this.zkc) {
            int zkSessionTimeout = conf.getBKClientZKSessionTimeoutMilliSeconds();
            RetryPolicy retryPolicy = null;
            if (conf.getBKClientZKNumRetries() > 0) {
                retryPolicy = new BoundExponentialBackoffRetryPolicy(
                        conf.getBKClientZKRetryBackoffStartMillis(),
                        conf.getBKClientZKRetryBackoffMaxMillis(), conf.getBKClientZKNumRetries());
            }

            Credentials credentials = Credentials.NONE;
            if (conf.getZkAclId() != null) {
                credentials = new DigestCredentials(conf.getZkAclId(), conf.getZkAclId());
            }

            this.zkc = new ZooKeeperClient(name + ":zk", zkSessionTimeout, 2 * zkSessionTimeout, zkServers,
                                           retryPolicy, statsLogger.scope("bkc_zkc"), conf.getZKClientNumberRetryThreads(),
                                           conf.getBKClientZKRequestRateLimit(), credentials);
        }
        registerExpirationHandler = conf.getBKClientZKNumRetries() <= 0;

        try {
            commonInitialization(conf, ledgersPath, channelFactory, statsLogger, requestTimer, registerExpirationHandler);
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Interrupted on creating bookkeeper client " + name + " : ", e);
        } catch (KeeperException e) {
            throw new ZKException("Error on creating bookkeeper client " + name + " : ", e);
        }

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
        if (null == bkc) {
            initialize();
        }
        return bkc;
    }

    public synchronized void close() {
        if (closed) {
            return;
        }

        LOG.info("BookKeeper Client closed {}", name);
        if (null != bkc) {
            try {
                bkc.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted on closing bookkeeper client {} : ", name, e);
                Thread.currentThread().interrupt();
            } catch (BKException e) {
                LOG.warn("Error on closing bookkeeper client {} : ", name, e);
            }
        }
        if (null != zkc) {
            if (null != sessionExpireWatcher) {
                zkc.unregister(sessionExpireWatcher);
            }
            if (ownZK) {
                zkc.close();
            }
        }
        closed = true;
    }

    @Override
    public void notifySessionExpired() {
        zkSessionExpired.set(true);
    }

    public synchronized void checkClosedOrInError() throws AlreadyClosedException {
        if (closed) {
            LOG.error("BookKeeper Client {} is already closed", name);
            throw new AlreadyClosedException("BookKeeper Client " + name + " is already closed");
        }

        if (zkSessionExpired.get()) {
            LOG.error("BookKeeper Client {}'s Zookeeper session has expired", name);
            throw new AlreadyClosedException("BookKeeper Client " + name + "'s Zookeeper session has expired");
        }
    }
}