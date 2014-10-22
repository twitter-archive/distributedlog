package com.twitter.distributedlog.acl;

import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.ZooKeeperClusterTestCase;
import com.twitter.distributedlog.thrift.AccessControlEntry;
import com.twitter.util.Await;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

public class TestZKAccessControl extends ZooKeeperClusterTestCase {

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder.newBuilder()
                .uri(createURI("/"))
                .zkAclId(null)
                .sessionTimeoutMs(10000).build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    private URI createURI(String path) {
        return URI.create("distributedlog://127.0.0.1:7000" + path);
    }

    @Test(timeout = 60000)
    public void testCreateZKAccessControl() throws Exception {
        AccessControlEntry ace = new AccessControlEntry();
        ace.setDenyWrite(true);
        String zkPath = "/create-zk-access-control";
        ZKAccessControl zkac = new ZKAccessControl(ace, zkPath);
        Await.result(zkac.create(zkc));

        ZKAccessControl readZKAC = Await.result(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(zkac, readZKAC);

        ZKAccessControl another = new ZKAccessControl(ace, zkPath);
        try {
            Await.result(another.create(zkc));
        } catch (KeeperException.NodeExistsException ke) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testDeleteZKAccessControl() throws Exception {
        String zkPath = "/delete-zk-access-control";

        AccessControlEntry ace = new AccessControlEntry();
        ace.setDenyDelete(true);

        ZKAccessControl zkac = new ZKAccessControl(ace, zkPath);
        Await.result(zkac.create(zkc));

        ZKAccessControl readZKAC = Await.result(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(zkac, readZKAC);

        Await.result(ZKAccessControl.delete(zkc, zkPath));

        try {
            Await.result(ZKAccessControl.read(zkc, zkPath, null));
        } catch (KeeperException.NoNodeException nne) {
            // expected.
        }
        Await.result(ZKAccessControl.delete(zkc, zkPath));
    }

    @Test(timeout = 60000)
    public void testEmptyZKAccessControl() throws Exception {
        String zkPath = "/empty-access-control";

        zkc.get().create(zkPath, new byte[0], zkc.getDefaultACL(), CreateMode.PERSISTENT);

        ZKAccessControl readZKAC = Await.result(ZKAccessControl.read(zkc, zkPath, null));

        assertEquals(zkPath, readZKAC.zkPath);
        assertEquals(ZKAccessControl.DEFAULT_ACCESS_CONTROL_ENTRY, readZKAC.getAccessControlEntry());
        assertTrue(ZKAccessControl.DEFAULT_ACCESS_CONTROL_ENTRY == readZKAC.getAccessControlEntry());
    }

    @Test(timeout = 60000)
    public void testCorruptedZKAccessControl() throws Exception {
        String zkPath = "/corrupted-zk-access-control";

        zkc.get().create(zkPath, "corrupted-data".getBytes(UTF_8), zkc.getDefaultACL(), CreateMode.PERSISTENT);

        try {
            Await.result(ZKAccessControl.read(zkc, zkPath, null));
        } catch (ZKAccessControl.CorruptedAccessControlException cace) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testUpdateZKAccessControl() throws Exception {
        String zkPath = "/update-zk-access-control";

        AccessControlEntry ace = new AccessControlEntry();
        ace.setDenyDelete(true);

        ZKAccessControl zkac = new ZKAccessControl(ace, zkPath);
        Await.result(zkac.create(zkc));

        ZKAccessControl readZKAC = Await.result(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(zkac, readZKAC);

        ace.setDenyRelease(true);
        ZKAccessControl newZKAC = new ZKAccessControl(ace, zkPath);
        Await.result(newZKAC.update(zkc));
        ZKAccessControl readZKAC2 = Await.result(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(newZKAC, readZKAC2);

        try {
            Await.result(readZKAC.update(zkc));
        } catch (KeeperException.BadVersionException bve) {
            // expected
        }
        readZKAC2.accessControlEntry.setDenyTruncate(true);
        Await.result(readZKAC2.update(zkc));
        ZKAccessControl readZKAC3 = Await.result(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(readZKAC2, readZKAC3);
    }
}
