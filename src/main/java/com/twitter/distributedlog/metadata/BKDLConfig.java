package com.twitter.distributedlog.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.thrift.BKDLConfigFormat;
import com.twitter.distributedlog.util.DLUtils;
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
        dlConf.setEncodeRegionIDInVersion(bkdlConfig.getEncodeRegionID());
        LOG.info("Propagate BKDLConfig to DLConfig : sanityCheckTxnID = {}, encodeRegionID = {}.",
                dlConf.getSanityCheckTxnID(), dlConf.getEncodeRegionIDInVersion());
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

    private String bkZkServersForWriter;
    private String bkZkServersForReader;
    private String bkLedgersPath;
    private boolean sanityCheckTxnID = true;
    private boolean encodeRegionID = false;
    private String dlZkServersForWriter;
    private String dlZkServersForReader;
    private String aclRootPath;

    /**
     * Construct a empty config with given <i>uri</i>.
     */
    BKDLConfig(URI uri) {
        this(DLUtils.getZKServersFromDLUri(uri),
             DLUtils.getZKServersFromDLUri(uri),
             null, null, null);
    }

    /**
     * The caller should make sure both dl and bk use same zookeeper server.
     *
     * @param zkServers
     *          zk servers used for both dl and bk.
     * @param ledgersPath
     *          ledgers path.
     */
    @VisibleForTesting
    public BKDLConfig(String zkServers, String ledgersPath) {
        this(zkServers, zkServers, zkServers, zkServers, ledgersPath);
    }

    public BKDLConfig(String dlZkServersForWriter,
                      String dlZkServersForReader,
                      String bkZkServersForWriter,
                      String bkZkServersForReader,
                      String bkLedgersPath) {
        this.dlZkServersForWriter = dlZkServersForWriter;
        this.dlZkServersForReader = dlZkServersForReader;
        this.bkZkServersForWriter = bkZkServersForWriter;
        this.bkZkServersForReader = bkZkServersForReader;
        this.bkLedgersPath = bkLedgersPath;
    }

    /**
     * @return zk servers used for bk for writers
     */
    public String getBkZkServersForWriter() {
        return bkZkServersForWriter;
    }

    /**
     * @return zk servers used for bk for readers
     */
    public String getBkZkServersForReader() {
        return bkZkServersForReader;
    }

    /**
     * @return zk servers used for dl for writers
     */
    public String getDlZkServersForWriter() {
        return dlZkServersForWriter;
    }

    /**
     * @return zk servers used for dl for readers
     */
    public String getDlZkServersForReader() {
        return dlZkServersForReader;
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

    /**
     * Enable/Disable encode region id.
     *
     * @param enabled
     *          flag to enable/disable encoding region id.
     * @return bk dl config
     */
    public BKDLConfig setEncodeRegionID(boolean enabled) {
        this.encodeRegionID = enabled;
        return this;
    }

    /**
     * @return flag to encode region id.
     */
    public boolean getEncodeRegionID() {
        return encodeRegionID;
    }

    /**
     * Set the root path of zk based ACL manager.
     *
     * @param aclRootPath
     *          root path of zk based ACL manager.
     * @return bk dl config
     */
    public BKDLConfig setACLRootPath(String aclRootPath) {
        this.aclRootPath = aclRootPath;
        return this;
    }

    /**
     * Get the root path of zk based ACL manager.
     *
     * @return root path of zk based ACL manager.
     */
    public String getACLRootPath() {
        return aclRootPath;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(bkZkServersForWriter, bkZkServersForReader,
                                dlZkServersForWriter, dlZkServersForReader,
                                bkLedgersPath);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BKDLConfig)) {
            return false;
        }
        BKDLConfig another = (BKDLConfig) o;
        return Objects.equal(bkZkServersForWriter, another.bkZkServersForWriter) &&
               Objects.equal(bkZkServersForReader, another.bkZkServersForReader) &&
               Objects.equal(dlZkServersForWriter, another.dlZkServersForWriter) &&
               Objects.equal(dlZkServersForReader, another.dlZkServersForReader) &&
               Objects.equal(bkLedgersPath, another.bkLedgersPath) &&
               sanityCheckTxnID == another.sanityCheckTxnID &&
               encodeRegionID == another.encodeRegionID &&
               Objects.equal(aclRootPath, another.aclRootPath);
    }

    @Override
    public String toString() {
        return serialize();
    }

    @Override
    public String serialize() {
        BKDLConfigFormat configFormat = new BKDLConfigFormat();
        if (null != bkZkServersForWriter) {
            configFormat.setBkZkServers(bkZkServersForWriter);
        }
        if (null != bkZkServersForReader) {
            configFormat.setBkZkServersForReader(bkZkServersForReader);
        }
        if (null != dlZkServersForWriter) {
            configFormat.setDlZkServersForWriter(dlZkServersForWriter);
        }
        if (null != dlZkServersForReader) {
            configFormat.setDlZkServersForReader(dlZkServersForReader);
        }
        if (null != bkLedgersPath) {
            configFormat.setBkLedgersPath(bkLedgersPath);
        }
        configFormat.setSanityCheckTxnID(sanityCheckTxnID);
        configFormat.setEncodeRegionID(encodeRegionID);
        if (null != aclRootPath) {
            configFormat.setAclRootPath(aclRootPath);
        }
        return serialize(configFormat);
    }

    String serialize(BKDLConfigFormat configFormat) {
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
        // bookkeeper cluster settings
        if (configFormat.isSetBkZkServers()) {
            bkZkServersForWriter = configFormat.getBkZkServers();
        }
        if (configFormat.isSetBkZkServersForReader()) {
            bkZkServersForReader = configFormat.getBkZkServersForReader();
        } else {
            bkZkServersForReader = bkZkServersForWriter;
        }
        if (configFormat.isSetBkLedgersPath()) {
            bkLedgersPath = configFormat.getBkLedgersPath();
        }
        // dl zookeeper cluster settings
        if (configFormat.isSetDlZkServersForWriter()) {
            dlZkServersForWriter = configFormat.getDlZkServersForWriter();
        }
        if (configFormat.isSetDlZkServersForReader()) {
            dlZkServersForReader = configFormat.getDlZkServersForReader();
        } else {
            dlZkServersForReader = dlZkServersForWriter;
        }
        // dl settings
        sanityCheckTxnID = !configFormat.isSetSanityCheckTxnID() || configFormat.isSanityCheckTxnID();
        encodeRegionID = configFormat.isSetEncodeRegionID() && configFormat.isEncodeRegionID();
        if (configFormat.isSetAclRootPath()) {
            aclRootPath = configFormat.getAclRootPath();
        }

        // Validate the settings
        if (null == bkZkServersForWriter || null == bkZkServersForReader || null == bkLedgersPath ||
            null == dlZkServersForWriter || null == dlZkServersForReader) {
            throw new IOException("Missing zk/bk settings in BKDL Config : " + new String(data, UTF_8));
        }
    }
}
