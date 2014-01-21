package com.twitter.distributedlog.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.thrift.BKDLConfigFormat;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Configurations for BookKeeper based DL.
 */
public class BKDLConfig implements DLConfig {

    private static final Logger LOG = LoggerFactory.getLogger(BKDLConfig.class);

    private static final int BUFFER_SIZE = 4096;
    private static final ConcurrentMap<URI, DLConfig> cachedDLConfigs =
            new ConcurrentHashMap<URI, DLConfig>();

    public static void propagateConfiguration(BKDLConfig bkdlConfig, DistributedLogConfiguration dlConf) {
        dlConf.setSanityCheckTxnID(bkdlConfig.getSanityCheckTxnID());
        LOG.info("Propagate BKDLConfig to DLConfig : sanityCheckTxnID = {}.", dlConf.getSanityCheckTxnID());
    }

    public static BKDLConfig resolveDLConfig(ZooKeeperClient zkc, URI uri) throws IOException {
        DLConfig dlConfig = cachedDLConfigs.get(uri);
        if (dlConfig == null) {
            dlConfig = (new ZkMetadataResolver(zkc).resolve(uri)).getDLConfig();
            DLConfig oldDLConfig = cachedDLConfigs.putIfAbsent(uri, dlConfig);
            if (null != oldDLConfig) {
                dlConfig = oldDLConfig;
            }
        }
        assert (dlConfig instanceof BKDLConfig);
        return (BKDLConfig)dlConfig;
    }

    @VisibleForTesting
    public static void clearCachedDLConfigs() {
        cachedDLConfigs.clear();
    }

    private String zkServers;
    private String bkLedgersPath;
    private boolean sanityCheckTxnID = true;

    /**
     * Construct a empty config.
     */
    BKDLConfig() {
        this(null, null);
    }

    public BKDLConfig(String zkServers, String bkLedgersPath) {
        this.zkServers = zkServers;
        this.bkLedgersPath = bkLedgersPath;
    }

    /**
     * @return zk servers used for bk
     */
    public String getZkServers() {
        return zkServers;
    }

    /**
     * @return ledgers path for bk
     */
    public String getBkLedgersPath() {
        return bkLedgersPath;
    }

    /**
     * Enable/Disable sanity check txn id.
     *
     * @param enabled
     *          flag to enable/disable sanity check txn id.
     * @return bk dl config.
     */
    public BKDLConfig setSanityCheckTxnID(boolean enabled) {
        this.sanityCheckTxnID = enabled;
        return this;
    }

    /**
     * @return flag to sanity check highest txn id.
     */
    public boolean getSanityCheckTxnID() {
        return sanityCheckTxnID;
    }

    @Override
    public int hashCode() {
        return (null == zkServers ? 0 : zkServers.hashCode()) * 13 +
                (null == bkLedgersPath ? 0 : bkLedgersPath.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BKDLConfig)) {
            return false;
        }
        BKDLConfig another = (BKDLConfig) o;
        boolean res;
        if (zkServers == null) {
            res = another.zkServers == null;
        } else {
            res = zkServers.equals(another.zkServers);
        }
        if (!res) {
            return false;
        }
        if (bkLedgersPath == null) {
            res = another.bkLedgersPath == null;
        } else {
            return bkLedgersPath.equals(another.bkLedgersPath);
        }
        return res && sanityCheckTxnID == another.sanityCheckTxnID;
    }

    @Override
    public String toString() {
        return serialize();
    }

    @Override
    public String serialize() {
        BKDLConfigFormat configFormat = new BKDLConfigFormat();
        if (null != zkServers) {
            configFormat.setBkZkServers(zkServers);
        }
        if (null != bkLedgersPath) {
            configFormat.setBkLedgersPath(bkLedgersPath);
        }
        configFormat.setSanityCheckTxnID(sanityCheckTxnID);
        TMemoryBuffer transport = new TMemoryBuffer(BUFFER_SIZE);
        TJSONProtocol protocol = new TJSONProtocol(transport);
        try {
            configFormat.write(protocol);
            transport.flush();
            return transport.toString("UTF-8");
        } catch (TException e) {
            throw new RuntimeException("Failed to serialize BKDLConfig : ", e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to serialize BKDLConfig : ", e);
        }
    }

    @Override
    public void deserialize(byte[] data) throws IOException {
        BKDLConfigFormat configFormat = new BKDLConfigFormat();
        TMemoryInputTransport transport = new TMemoryInputTransport(data);
        TJSONProtocol protocol = new TJSONProtocol(transport);
        try {
            configFormat.read(protocol);
        } catch (TException e) {
            throw new IOException("Failed to deserialize data '" +
                    new String(data, UTF_8) + "' : ", e);
        }
        if (configFormat.isSetBkZkServers()) {
            zkServers = configFormat.getBkZkServers();
        }
        if (configFormat.isSetBkLedgersPath()) {
            bkLedgersPath = configFormat.getBkLedgersPath();
        }
        if (configFormat.isSetSanityCheckTxnID()) {
            sanityCheckTxnID = configFormat.isSanityCheckTxnID();
        }
    }
}
