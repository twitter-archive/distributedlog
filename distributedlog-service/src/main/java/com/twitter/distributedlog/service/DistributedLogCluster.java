package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.twitter.finagle.builder.Server;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * DistributedLog Cluster is an emulator to run distributedlog components.
 */
public class DistributedLogCluster {

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build distributedlog cluster.
     */
    public static class Builder {

        int _numBookies = 3;
        boolean _shouldStartZK = true;
        String _zkHost = "127.0.0.1";
        int _zkPort = 0;
        boolean _shouldStartProxy = true;
        int _proxyPort = 7000;
        DistributedLogConfiguration _conf = new DistributedLogConfiguration()
                .setLockTimeout(10)
                .setOutputBufferSize(0)
                .setImmediateFlushEnabled(true);

        private Builder() {}

        /**
         * How many bookies to run. By default is 3.
         *
         * @return builder
         */
        public Builder numBookies(int numBookies) {
            this._numBookies = numBookies;
            return this;
        }

        /**
         * Whether to start zookeeper? By default is true.
         *
         * @param startZK
         *          flag to start zookeeper?
         * @return builder
         */
        public Builder shouldStartZK(boolean startZK) {
            this._shouldStartZK = startZK;
            return this;
        }

        /**
         * ZooKeeper server to run. By default it runs locally on '127.0.0.1'.
         *
         * @param zkServers
         *          zk servers
         * @return builder
         */
        public Builder zkServers(String zkServers) {
            this._zkHost = zkServers;
            return this;
        }

        /**
         * ZooKeeper server port to listen on. By default it listens on 2181.
         *
         * @param zkPort
         *          zookeeper server port.
         * @return builder.
         */
        public Builder zkPort(int zkPort) {
            this._zkPort = zkPort;
            return this;
        }

        /**
         * Whether to start proxy or not. By default is true.
         *
         * @param startProxy
         *          whether to start proxy or not.
         * @return builder
         */
        public Builder shouldStartProxy(boolean startProxy) {
            this._shouldStartProxy = startProxy;
            return this;
        }

        /**
         * Port that proxy server to listen on. By default is 7000.
         *
         * @param proxyPort
         *          port that proxy server to listen on.
         * @return builder
         */
        public Builder proxyPort(int proxyPort) {
            this._proxyPort = proxyPort;
            return this;
        }

        /**
         * DistributedLog Configuration
         *
         * @param conf
         *          distributedlog configuration
         * @return builder
         */
        public Builder conf(DistributedLogConfiguration conf) {
            this._conf = conf;
            return this;
        }

        public DistributedLogCluster build() throws Exception {
            // build the cluster
            return new DistributedLogCluster(
                    _conf,
                    _numBookies,
                    _shouldStartZK,
                    _zkHost,
                    _zkPort,
                    _shouldStartProxy,
                    _proxyPort);
        }
    }

    public static class DLServer {

        static final int MAX_RETRIES = 20;
        static final int MIN_PORT = 1025;
        static final int MAX_PORT = 65535;

        int proxyPort;

        public final InetSocketAddress address;
        public final Pair<DistributedLogServiceImpl, Server> dlServer;

        protected DLServer(DistributedLogConfiguration conf, URI uri, int basePort) throws Exception {
            proxyPort = basePort;

            boolean success = false;
            int retries = 0;
            Pair<DistributedLogServiceImpl, Server> serverPair = null;
            while (!success) {
                try {
                    serverPair = DistributedLogServer.runServer(conf, uri, new NullStatsProvider(), proxyPort);
                    success = true;
                } catch (BindException be) {
                    retries++;
                    if (retries > MAX_RETRIES) {
                        throw be;
                    }
                    proxyPort++;
                    if (proxyPort > MAX_PORT) {
                        proxyPort = MIN_PORT;
                    }
                }
            }
            dlServer = serverPair;
            address = DLSocketAddress.getSocketAddress(proxyPort);
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        public void shutdown() {
            DistributedLogServer.closeServer(dlServer);
        }
    }

    private final DistributedLogConfiguration conf;
    private final ZooKeeperServerShim zks;
    private final LocalDLMEmulator dlmEmulator;
    private DLServer dlServer;
    private final boolean shouldStartProxy;
    private final int proxyPort;
    private final List<File> tmpDirs = new ArrayList<File>();

    private DistributedLogCluster(DistributedLogConfiguration conf,
                                  int numBookies,
                                  boolean shouldStartZK,
                                  String zkServers,
                                  int zkPort,
                                  boolean shouldStartProxy,
                                  int proxyPort) throws Exception {
        this.conf = conf;
        if (shouldStartZK) {
            File zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
            tmpDirs.add(zkTmpDir);
            Pair<ZooKeeperServerShim, Integer> serverAndPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkTmpDir);
            this.zks = serverAndPort.getLeft();
            zkPort = serverAndPort.getRight();
        } else {
            this.zks = null;
        }
        this.dlmEmulator = new LocalDLMEmulator(numBookies, zkServers, zkPort);
        this.shouldStartProxy = shouldStartProxy;
        this.proxyPort = proxyPort;
    }

    public void start() throws Exception {
        this.dlmEmulator.start();
        BKDLConfig bkdlConfig = new BKDLConfig(this.dlmEmulator.getZkServers(), "/ledgers").setACLRootPath(".acl");
        DLMetadata.create(bkdlConfig).update(this.dlmEmulator.getUri());
        if (shouldStartProxy) {
            this.dlServer = new DLServer(conf, this.dlmEmulator.getUri(), proxyPort);
        } else {
            this.dlServer = null;
        }
    }

    public void stop() throws Exception {
        if (null != dlServer) {
            this.dlServer.shutdown();
        }
        this.dlmEmulator.teardown();
        if (null != this.zks) {
            this.zks.stop();
        }
        for (File dir : tmpDirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    public URI getUri() {
        return this.dlmEmulator.getUri();
    }

    public String getZkServers() {
        return this.dlmEmulator.getZkServers();
    }

    public String getProxyFinagleStr() {
        return "inet!" + (dlServer == null ? "127.0.0.1:" + proxyPort : dlServer.getAddress().toString());
    }

}
