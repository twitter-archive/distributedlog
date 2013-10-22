package com.twitter.distributedlog.metadata;

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
        dlMetadata.create(createURI("/messaging/distributedlog"));
        DLMetadata dlMetadata2 = DLMetadata.create(bkdlConfig2);
        dlMetadata2.create(createURI("/messaging/distributedlog/child"));
        assertEquals(dlMetadata,
                resolver.resolve(createURI("/messaging/distributedlog")));
        assertEquals(dlMetadata2,
                resolver.resolve(createURI("/messaging/distributedlog/child")));
        assertEquals(dlMetadata2,
                resolver.resolve(createURI("/messaging/distributedlog/child/unknown")));
        Utils.zkCreateFullPathOptimistic(zkc, "/messaging/distributedlog/child/child2", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(dlMetadata2,
                resolver.resolve(createURI("/messaging/distributedlog/child/child2")));
    }

}
