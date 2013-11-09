package com.twitter.distributedlog;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.metadata.BKDLConfig;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Builder to build bookkeeper client.
 */
class BookKeeperClientBuilder {

    /**
     * Create a bookkeeper client builder to build bookkeeper clients.
     *
     * @return bookkeeper client builder.
     */
    public static BookKeeperClientBuilder newBuilder() {
        return new BookKeeperClientBuilder();
    }

    // client name
    private String name = null;
    // zookeeper client
    private ZooKeeperClient zkc = null;
    // dl config
    private DistributedLogConfiguration dlConfig = null;
    // bkdl config
    private BKDLConfig bkdlConfig = null;
    // statsLogger
    private StatsLogger statsLogger = NullStatsLogger.INSTANCE;

    // Cached BookKeeper Client
    private BookKeeperClient cachedClient = null;

    /**
     * Private bookkeeper builder.
     */
    private BookKeeperClientBuilder() {}

    /**
     * Set client name.
     *
     * @param name
     *          client name.
     * @return builder
     */
    public synchronized BookKeeperClientBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     * <i>bkdlConfig</i> used to configure bookkeeper. It is different with
     * {@link #dlConfig(DistributedLogConfiguration)}. {@link BKDLConfig} is
     * used to store state-full configurations (e.g. zkServers & ledgersPath),
     * while {@link DistributedLogConfiguration} is used to store state-less
     * configurations (e.g. timeout).
     *
     * @param bkdlConfig
     *          bkdl config.
     * @return builder
     */
    public synchronized BookKeeperClientBuilder bkdlConfig(BKDLConfig bkdlConfig) {
        this.bkdlConfig = bkdlConfig;
        return this;
    }

    /**
     * <i>dlConfig</i> used to configure bookkeeper client.
     * @see {@link #bkdlConfig(BKDLConfig)}
     *
     * @param dlConfig
     *          distributedlog config.
     * @return builder.
     */
    public synchronized BookKeeperClientBuilder dlConfig(DistributedLogConfiguration dlConfig) {
        this.dlConfig = dlConfig;
        return this;
    }

    /**
     * Set the zkc used to build bookkeeper client.
     *
     * @param zkc
     *          zookeeper client.
     * @return builder
     */
    public synchronized BookKeeperClientBuilder zkc(ZooKeeperClient zkc) {
        this.zkc = zkc;
        return this;
    }

    /**
     * Build BookKeeper client using existing <i>bkc</i> client.
     *
     * @param bkc
     *          bookkeeper client.
     * @return builder
     */
    public synchronized BookKeeperClientBuilder bkc(BookKeeperClient bkc) {
        this.cachedClient = bkc;
        return this;
    }

    /**
     * Build BookKeeper Client using given stats logger <i>statsLogger</i>.
     *
     * @param statsLogger
     *          stats logger to report stats
     * @return builder.
     */
    public synchronized BookKeeperClientBuilder statsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        return this;
    }

    private void validateParameters() {
        Preconditions.checkNotNull(name, "Missing client name.");
        Preconditions.checkNotNull(dlConfig, "Missing DistributedLog Configuration.");
        Preconditions.checkNotNull(bkdlConfig, "Missing BKDL Config.");
    }

    public synchronized BookKeeperClient build()
            throws InterruptedException, IOException, KeeperException {
        if (null == cachedClient) {
            cachedClient = buildClient();
        } else {
            cachedClient.addRef();
        }
        return cachedClient;
    }

    private BookKeeperClient buildClient()
            throws InterruptedException, IOException, KeeperException {
        validateParameters();
        if (null == zkc) {
            return new BookKeeperClient(dlConfig, bkdlConfig, name, statsLogger);
        } else {
            return new BookKeeperClient(dlConfig, bkdlConfig, zkc, name, statsLogger);
        }
    }
}
