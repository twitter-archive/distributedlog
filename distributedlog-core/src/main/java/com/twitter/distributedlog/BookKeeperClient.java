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

import com.twitter.distributedlog.ZooKeeperClient.Credentials;
import com.twitter.distributedlog.ZooKeeperClient.DigestCredentials;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.net.NetUtils;
import com.twitter.distributedlog.util.ConfUtils;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;

import static com.google.common.base.Charsets.UTF_8;

/**
 * BookKeeper Client wrapper over {@link BookKeeper}.
 *
 * <h3>Metrics</h3>
 * <ul>
 * <li> bookkeeper operation stats are exposed under current scope by {@link BookKeeper}
 * </ul>
 */
public class BookKeeperClient implements ZooKeeperClient.ZooKeeperSessionExpireNotifier {
    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClient.class);

    // Parameters to build bookkeeper client
    private final DistributedLogConfiguration conf;
    private final String name;
    private final String zkServers;
    private final String ledgersPath;
    private final byte[] passwd;
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

    @SuppressWarnings("deprecation")
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

        Class<? extends DNSToSwitchMapping> dnsResolverCls;
        try {
            dnsResolverCls = conf.getEnsemblePlacementDnsResolverClass();
        } catch (ConfigurationException e) {
            LOG.error("Failed to load bk dns resolver : ", e);
            throw new IOException("Failed to load bk dns resolver : ", e);
        }
        final DNSToSwitchMapping dnsResolver =
                NetUtils.getDNSResolver(dnsResolverCls, conf.getBkDNSResolverOverrides());

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
        this.passwd = conf.getBKDigestPW().getBytes(UTF_8);
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

    // Util functions
    public Future<LedgerHandle> createLedger(int ensembleSize,
                                             int writeQuorumSize,
                                             int ackQuorumSize) {
        BookKeeper bk;
        try {
            bk = get();
        } catch (IOException ioe) {
            return Future.exception(ioe);
        }
        final Promise<LedgerHandle> promise = new Promise<LedgerHandle>();
        bk.asyncCreateLedger(ensembleSize, writeQuorumSize, ackQuorumSize,
                BookKeeper.DigestType.CRC32, passwd, new AsyncCallback.CreateCallback() {
                    @Override
                    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                        if (BKException.Code.OK == rc) {
                            promise.updateIfEmpty(new Return<LedgerHandle>(lh));
                        } else {
                            promise.updateIfEmpty(new Throw<LedgerHandle>(BKException.create(rc)));
                        }
                    }
                }, null);
        return promise;
    }

    public Future<Void> deleteLedger(long lid,
                                     final boolean ignoreNonExistentLedger) {
        BookKeeper bk;
        try {
            bk = get();
        } catch (IOException ioe) {
            return Future.exception(ioe);
        }
        final Promise<Void> promise = new Promise<Void>();
        bk.asyncDeleteLedger(lid, new AsyncCallback.DeleteCallback() {
            @Override
            public void deleteComplete(int rc, Object ctx) {
                if (BKException.Code.OK == rc) {
                    promise.updateIfEmpty(new Return<Void>(null));
                } else if (BKException.Code.NoSuchLedgerExistsException == rc) {
                    if (ignoreNonExistentLedger) {
                        promise.updateIfEmpty(new Return<Void>(null));
                    } else {
                        promise.updateIfEmpty(new Throw<Void>(BKException.create(rc)));
                    }
                } else {
                    promise.updateIfEmpty(new Throw<Void>(BKException.create(rc)));
                }
            }
        }, null);
        return promise;
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
