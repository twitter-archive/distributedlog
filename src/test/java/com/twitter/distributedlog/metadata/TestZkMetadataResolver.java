package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.Utils;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestZkMetadataResolver {

    private static final BKDLConfig bkdlConfig = new BKDLConfig("127.0.0.1:7000", "ledgers");
    private static final BKDLConfig bkdlConfig2 = new BKDLConfig("127.0.0.1:7000", "ledgers2");

    private ZooKeeperClient zkc;
    private ZkMetadataResolver resolver;
    private static ZooKeeperServerShim zks;

    @BeforeClass
    public static void setupZooKeeper() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
    }

    @AfterClass
    public static void shutdownZooKeeper() throws Exception {
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .uri(createURI("/"))
                .sessionTimeoutMs(10000).build();
        resolver = new ZkMetadataResolver(zkc);
    }

    @After
    public void tearDown() throws Exception {
        zkc.close();
    }

    private URI createURI(String path) {
        return URI.create("distributedlog://127.0.0.1:7000" + path);
    }

    @Test(timeout = 60000)
    public void testResolveFailures() throws Exception {
        // resolve unexisted path
        try {
            resolver.resolve(createURI("/unexisted/path"));
            fail("Should fail if no metadata resolved.");
        } catch (IOException e) {
            // expected
        }
        // resolve existed unbound path
        Utils.zkCreateFullPathOptimistic(zkc, "/existed/path", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            resolver.resolve(createURI("/existed/path"));
            fail("Should fail if no metadata resolved.");
        } catch (IOException e) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testResolve() throws Exception {
        DLMetadata dlMetadata = DLMetadata.create(bkdlConfig);
        dlMetadata.create(createURI("/messaging/distributedlog-testresolve"));
        DLMetadata dlMetadata2 = DLMetadata.create(bkdlConfig2);
        dlMetadata2.create(createURI("/messaging/distributedlog-testresolve/child"));
        assertEquals(dlMetadata,
                resolver.resolve(createURI("/messaging/distributedlog-testresolve")));
        assertEquals(dlMetadata2,
                resolver.resolve(createURI("/messaging/distributedlog-testresolve/child")));
        assertEquals(dlMetadata2,
                resolver.resolve(createURI("/messaging/distributedlog-testresolve/child/unknown")));
        Utils.zkCreateFullPathOptimistic(zkc, "/messaging/distributedlog-testresolve/child/child2", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(dlMetadata2,
                resolver.resolve(createURI("/messaging/distributedlog-testresolve/child/child2")));
    }

    @Test(timeout = 60000)
    public void testEncodeRegionID() throws Exception {
        DistributedLogConfiguration dlConf = new DistributedLogConfiguration();

        URI uri = createURI("/messaging/distributedlog-testencoderegionid/dl1");
        DLMetadata meta1 = DLMetadata.create(new BKDLConfig("127.0.0.1:7000", "ledgers"));
        meta1.create(uri);
        BKDLConfig read1 = BKDLConfig.resolveDLConfig(zkc, uri);
        BKDLConfig.propagateConfiguration(read1, dlConf);
        assertFalse(dlConf.getEncodeRegionIDInVersion());

        BKDLConfig.clearCachedDLConfigs();

        DLMetadata meta2 = DLMetadata.create(new BKDLConfig("127.0.0.1:7000", "ledgers").setEncodeRegionID(true));
        meta2.update(uri);
        BKDLConfig read2 = BKDLConfig.resolveDLConfig(zkc, uri);
        BKDLConfig.propagateConfiguration(read2, dlConf);
        assertTrue(dlConf.getEncodeRegionIDInVersion());

        BKDLConfig.clearCachedDLConfigs();

        DLMetadata meta3 = DLMetadata.create(new BKDLConfig("127.0.0.1:7000", "ledgers").setEncodeRegionID(false));
        meta3.update(uri);
        BKDLConfig read3 = BKDLConfig.resolveDLConfig(zkc, uri);
        BKDLConfig.propagateConfiguration(read3, dlConf);
        assertFalse(dlConf.getEncodeRegionIDInVersion());

        BKDLConfig.clearCachedDLConfigs();
    }

}
