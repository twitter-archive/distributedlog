package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.thrift.ClientId$;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.SocketAddress;
import java.net.URI;

public abstract class DistributedLogServerTestCase {

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                    .setOutputBufferSize(0).setPeriodicFlushFrequencyMilliSeconds(10);
    protected static LocalDLMEmulator bkutil;
    protected static ZooKeeperServerShim zks;
    protected static URI uri;
    protected static int numBookies = 3;

    protected static class DLServer {
        final SocketAddress address;
        final Pair<DistributedLogServiceImpl, Server> dlServer;

        protected DLServer(int port) throws Exception {
            dlServer = DistributedLogServer.runServer(conf, uri, new NullStatsProvider(), port);
            address = DLSocketAddress.getSocketAddress(port);
        }

        SocketAddress getAddress() {
            return address;
        }

        public void shutdown() {
            DistributedLogServer.closeServer(dlServer);
        }
    }

    protected static class DLClient {
        final LocalRoutingService routingService;
        final DistributedLogClientBuilder.DistributedLogClientImpl dlClient;

        protected DLClient(String name) {
            routingService = new LocalRoutingService();
            dlClient = (DistributedLogClientBuilder.DistributedLogClientImpl)
                DistributedLogClientBuilder.newBuilder()
                        .name(name)
                        .clientId(ClientId$.MODULE$.apply(name))
                        .routingService(routingService)
                        .build();
        }

        public void shutdown() {
            dlClient.close();
        }
    }

    protected DLServer dlServer;
    protected DLClient dlClient;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
        uri = LocalDLMEmulator.createDLMURI("127.0.0.1:7000", "");
        BKDLConfig bkdlConfig = new BKDLConfig("127.0.0.1:7000", "/ledgers").setACLRootPath(".acl");
        DLMetadata.create(bkdlConfig).update(uri);
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        dlServer = createDistributedLogServer(7001);
        dlClient = createDistributedLogClient("test");
    }

    @After
    public void teardown() throws Exception {
        if (null != dlClient) {
            dlClient.shutdown();
        }
        if (null != dlServer) {
            dlServer.shutdown();
        }
    }

    DLServer createDistributedLogServer(int port) throws Exception {
        return new DLServer(port);
    }

    DLClient createDistributedLogClient(String name) throws Exception {
        return new DLClient(name);
    }
}
