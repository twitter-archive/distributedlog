package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.LocalDLMEmulator;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestDLMetadata {

    private static final BKDLConfig bkdlConfig = new BKDLConfig("127.0.0.1:7000", "ledgers");
    private static final BKDLConfig bkdlConfig2 = new BKDLConfig("127.0.0.1:7000", "ledgers2");

    private ZooKeeper zkc;
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
        zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", 7000);
    }

    @After
    public void tearDown() throws Exception {
        zkc.close();
    }

    private URI createURI(String path) {
        return URI.create("distributedlog://127.0.0.1:7000" + path);
    }

    @Test(timeout = 60000)
    public void testBadMetadata() throws Exception {
        try {
            DLMetadata.deserialize(new byte[0]);
            fail("Should fail to deserialize invalid metadata");
        } catch (IOException ie) {
            // expected
        }
        try {
            DLMetadata.deserialize(new DLMetadata("unknown", bkdlConfig).serialize());
            fail("Should fail to deserialize due to unknown dl type.");
        } catch (IOException ie) {
            // expected
        }
        try {
            DLMetadata.deserialize(new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig, 9999).serialize());
            fail("Should fail to deserialize due to invalid version.");
        } catch (IOException ie) {
            // expected
        }
        byte[] data = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig).serialize();
        // truncate data
        byte[] badData = new byte[data.length - 3];
        System.arraycopy(data, 0, badData, 0, badData.length);
        try {
            DLMetadata.deserialize(badData);
            fail("Should fail to deserialize truncated data.");
        } catch (IOException ie) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testGoodMetadata() throws Exception {
        byte[] data = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig).serialize();
        DLMetadata deserailized = DLMetadata.deserialize(data);
        assertEquals(bkdlConfig, deserailized.getDLConfig());
    }

    @Test(timeout = 60000)
    public void testWriteMetadata() throws Exception {
        DLMetadata metadata = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig);
        try {
            metadata.create(createURI("//metadata"));
            fail("Should fail due to invalid uri.");
        } catch (IllegalArgumentException e) {
            // expected
        }
        metadata.create(createURI("/metadata"));
        // create on existed path
        try {
            metadata.create(createURI("/metadata"));
            fail("Should fail when create on existed path");
        } catch (IOException e) {
            // expected
        }
        // update on unexisted path
        try {
            metadata.update(createURI("/unexisted"));
            fail("Should fail when update on unexisted path");
        } catch (IOException e) {
            // expected
        }
        byte[] data = zkc.getData("/metadata", false, new Stat());
        assertEquals(bkdlConfig, DLMetadata.deserialize(data).getDLConfig());
        // update on existed path
        DLMetadata newMetadata = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig2);
        newMetadata.update(createURI("/metadata"));
        byte[] newData = zkc.getData("/metadata", false, new Stat());
        assertEquals(bkdlConfig2, DLMetadata.deserialize(newData).getDLConfig());
    }
}
