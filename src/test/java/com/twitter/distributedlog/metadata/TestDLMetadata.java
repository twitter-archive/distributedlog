package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.ZooKeeperClusterTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestDLMetadata extends ZooKeeperClusterTestCase {

    private static final BKDLConfig bkdlConfig =
            new BKDLConfig("127.0.0.1:7000", "127.0.0.1:7000",
                           "127.0.0.1:7000", "127.0.0.1:7000", "ledgers");
    private static final BKDLConfig bkdlConfig2 =
            new BKDLConfig("127.0.0.1:7001", "127.0.0.1:7002",
                           "127.0.0.1:7003", "127.0.0.1:7004", "ledgers2");

    private ZooKeeper zkc;

    @Before
    public void setup() throws Exception {
        zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", 7000);
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    private URI createURI(String path) {
        return URI.create("distributedlog://127.0.0.1:7000" + path);
    }

    @Test(timeout = 60000)
    public void testBadMetadata() throws Exception {
        URI uri = createURI("/");
        try {
            DLMetadata.deserialize(uri, new byte[0]);
            fail("Should fail to deserialize invalid metadata");
        } catch (IOException ie) {
            // expected
        }
        try {
            DLMetadata.deserialize(uri, new DLMetadata("unknown", bkdlConfig).serialize());
            fail("Should fail to deserialize due to unknown dl type.");
        } catch (IOException ie) {
            // expected
        }
        try {
            DLMetadata.deserialize(uri, new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig, 9999).serialize());
            fail("Should fail to deserialize due to invalid version.");
        } catch (IOException ie) {
            // expected
        }
        byte[] data = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig).serialize();
        // truncate data
        byte[] badData = new byte[data.length - 3];
        System.arraycopy(data, 0, badData, 0, badData.length);
        try {
            DLMetadata.deserialize(uri, badData);
            fail("Should fail to deserialize truncated data.");
        } catch (IOException ie) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testGoodMetadata() throws Exception {
        URI uri = createURI("/");
        byte[] data = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig).serialize();
        DLMetadata deserailized = DLMetadata.deserialize(uri, data);
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
        URI uri = createURI("/metadata");
        metadata.create(uri);
        // create on existed path
        try {
            metadata.create(uri);
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
        assertEquals(bkdlConfig, DLMetadata.deserialize(uri, data).getDLConfig());
        // update on existed path
        DLMetadata newMetadata = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig2);
        newMetadata.update(createURI("/metadata"));
        byte[] newData = zkc.getData("/metadata", false, new Stat());
        assertEquals(bkdlConfig2, DLMetadata.deserialize(uri, newData).getDLConfig());
    }

    @Test(timeout = 60000)
    public void testMetadataWithoutDLZKServers() throws Exception {
        testMetadataWithOrWithoutZkServers(
                "/metadata-without-dlzk-servers",
                null, null, "127.0.0.1:7003", "127.0.0.1:7004",
                "127.0.0.1:7000", "127.0.0.1:7000", "127.0.0.1:7003", "127.0.0.1:7004");
    }

    @Test(timeout = 60000)
    public void testMetadataWithoutDLZKServersForRead() throws Exception {
        testMetadataWithOrWithoutZkServers(
                "/metadata-without-dlzk-servers-for-read",
                "127.0.0.1:7001", null, "127.0.0.1:7003", "127.0.0.1:7004",
                "127.0.0.1:7001", "127.0.0.1:7001", "127.0.0.1:7003", "127.0.0.1:7004");
    }

    @Test(timeout = 60000)
    public void testMetadataWithoutBKZKServersForRead() throws Exception {
        testMetadataWithOrWithoutZkServers(
                "/metadata-without-bkzk-servers-for-read",
                "127.0.0.1:7001", null, "127.0.0.1:7003", null,
                "127.0.0.1:7001", "127.0.0.1:7001", "127.0.0.1:7003", "127.0.0.1:7003");
    }

    private void testMetadataWithOrWithoutZkServers(
            String metadataPath,
            String dlZkServersForWriter, String dlZkServersForReader,
            String bkZkServersForWriter, String bkZkServersForReader,
            String expectedDlZkServersForWriter, String expectedDlZkServersForReader,
            String expectedBkZkServersForWriter, String expectedBkZkServersForReader
    ) throws Exception {
        BKDLConfig bkdlConfig = new BKDLConfig(dlZkServersForWriter, dlZkServersForReader,
                                               bkZkServersForWriter, bkZkServersForReader, "ledgers");
        BKDLConfig expectedBKDLConfig =
                new BKDLConfig(expectedDlZkServersForWriter, expectedDlZkServersForReader,
                               expectedBkZkServersForWriter, expectedBkZkServersForReader, "ledgers");
        URI uri = createURI(metadataPath);
        DLMetadata metadata = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig);
        metadata.create(uri);
        // read serialized metadata
        byte[] data = zkc.getData(metadataPath, false, new Stat());
        assertEquals(expectedBKDLConfig, DLMetadata.deserialize(uri, data).getDLConfig());
    }

    @Test(timeout = 60000)
    public void testMetadataMissingRequiredFields() throws Exception {
        BKDLConfig bkdlConfig = new BKDLConfig(null, null, null, null, "ledgers");
        URI uri = createURI("/metadata-missing-fields");
        DLMetadata metadata = new DLMetadata(DLMetadata.BK_DL_TYPE, bkdlConfig);
        metadata.create(uri);
        // read serialized metadata
        byte[] data = zkc.getData("/metadata-missing-fields", false, new Stat());
        try {
            DLMetadata.deserialize(uri, data);
            fail("Should fail on deserializing metadata missing fields");
        } catch (IOException ioe) {
            // expected
        }
    }
}
