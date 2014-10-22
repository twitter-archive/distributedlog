package com.twitter.distributedlog.acl;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.ZooKeeperClientUtils;
import com.twitter.distributedlog.ZooKeeperClusterTestCase;
import com.twitter.distributedlog.thrift.AccessControlEntry;
import com.twitter.util.Await;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;

public class TestZKAccessControlManager extends ZooKeeperClusterTestCase {

    static final Logger logger = LoggerFactory.getLogger(TestZKAccessControlManager.class);

    private DistributedLogConfiguration conf;
    private ZooKeeperClient zkc;
    private ScheduledExecutorService executorService;

    private URI createURI(String path) {
        return URI.create("distributedlog://127.0.0.1:7000" + path);
    }

    @Before
    public void setup() throws Exception {
        executorService = Executors.newSingleThreadScheduledExecutor();
        zkc = ZooKeeperClientBuilder.newBuilder()
                .uri(createURI("/"))
                .zkAclId(null)
                .sessionTimeoutMs(10000).build();
        conf = new DistributedLogConfiguration();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
        executorService.shutdown();
    }

    void setACL(ZKAccessControl accessControl) throws Exception {
        String zkPath = accessControl.getZKPath();
        if (null == zkc.get().exists(zkPath, false)) {
            accessControl.create(zkc);
        } else {
            accessControl.update(zkc);
        }
    }

    static void verifyStreamPermissions(ZKAccessControlManager zkcm,
                                        String stream,
                                        boolean allowWrite,
                                        boolean allowTruncate,
                                        boolean allowRelease,
                                        boolean allowDelete,
                                        boolean allowAcquire) throws Exception {
        assertEquals(allowWrite, zkcm.allowWrite(stream));
        assertEquals(allowTruncate, zkcm.allowTruncate(stream));
        assertEquals(allowRelease, zkcm.allowRelease(stream));
        assertEquals(allowDelete, zkcm.allowDelete(stream));
        assertEquals(allowAcquire, zkcm.allowAcquire(stream));
    }

    @Test(timeout = 60000)
    public void testZKAccessControlManager() throws Exception {
        String zkRootPath = "/test-zk-access-control-manager";
        String stream1 = "test-acm-1";
        String stream2 = "test-acm-2";
        logger.info("Creating ACL Manager for {}", zkRootPath);
        ZKAccessControlManager zkcm = new ZKAccessControlManager(conf, zkc, zkRootPath, executorService);
        logger.info("Created ACL Manager for {}", zkRootPath);
        try {
            verifyStreamPermissions(zkcm, stream1, true, true, true, true, true);

            // create stream1 (denyDelete = true)
            String zkPath1 = zkRootPath + "/" + stream1;
            AccessControlEntry ace1 = new AccessControlEntry();
            ace1.setDenyDelete(true);
            ZKAccessControl accessControl1 = new ZKAccessControl(ace1, zkPath1);
            setACL(accessControl1);
            logger.info("Create ACL for stream {} : {}", stream1, accessControl1);
            while (zkcm.allowDelete(stream1)) {
                Thread.sleep(100);
            }
            verifyStreamPermissions(zkcm, stream1, true, true, true, false, true);

            // update stream1 (denyDelete = false, denyWrite = true)
            ace1 = new AccessControlEntry();
            ace1.setDenyWrite(true);
            accessControl1 = new ZKAccessControl(ace1, zkPath1);
            setACL(accessControl1);
            logger.info("Update ACL for stream {} : {}", stream1, accessControl1);

            // create stream2 (denyTruncate = true)
            String zkPath2 = zkRootPath + "/" + stream2;
            AccessControlEntry ace2 = new AccessControlEntry();
            ace2.setDenyTruncate(true);
            ZKAccessControl accessControl2 = new ZKAccessControl(ace2, zkPath2);
            setACL(accessControl2);
            logger.info("Create ACL for stream {} : {}", stream2, accessControl2);
            while (zkcm.allowWrite(stream1)) {
                Thread.sleep(100);
            }
            while (zkcm.allowTruncate(stream2)) {
                Thread.sleep(100);
            }

            verifyStreamPermissions(zkcm, stream1, false, true, true, true, true);
            verifyStreamPermissions(zkcm, stream2, true, false, true, true, true);

            // delete stream2
            Await.result(ZKAccessControl.delete(zkc, zkPath2));
            logger.info("Delete ACL for stream {}", stream2);
            while (!zkcm.allowTruncate(stream2)) {
                Thread.sleep(100);
            }

            verifyStreamPermissions(zkcm, stream1, false, true, true, true, true);
            verifyStreamPermissions(zkcm, stream2, true, true, true, true, true);

            // expire session
            ZooKeeperClientUtils.expireSession(zkc, zkServers, 1000);

            // update stream1 (denyDelete = false, denyWrite = true)
            ace1 = new AccessControlEntry();
            ace1.setDenyRelease(true);
            accessControl1 = new ZKAccessControl(ace1, zkPath1);
            setACL(accessControl1);
            logger.info("Update ACL for stream {} : {}", stream1, accessControl1);

            // create stream2 (denyTruncate = true)
            ace2 = new AccessControlEntry();
            ace2.setDenyAcquire(true);
            accessControl2 = new ZKAccessControl(ace2, zkPath2);
            setACL(accessControl2);
            logger.info("Created ACL for stream {} again : {}", stream2, accessControl2);

            while (zkcm.allowRelease(stream1)) {
                Thread.sleep(100);
            }
            while (zkcm.allowAcquire(stream2)) {
                Thread.sleep(100);
            }

            verifyStreamPermissions(zkcm, stream1, true, true, false, true, true);
            verifyStreamPermissions(zkcm, stream2, true, true, true, true, false);
        } finally {
            zkcm.close();
        }
    }
}
