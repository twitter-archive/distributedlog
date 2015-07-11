package com.twitter.distributedlog.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestPermitManager {

    @Test
    public void testUnlimitedPermitManager() {
        PermitManager pm = PermitManager.UNLIMITED_PERMIT_MANAGER;
        List<PermitManager.Permit> permits = new ArrayList<PermitManager.Permit>();
        for (int i = 0; i < 10; i++) {
            permits.add(pm.acquirePermit());
        }
        for (int i = 0; i < 10; i++) {
            assertTrue(permits.get(i).isAllowed());
            pm.releasePermit(permits.get(i));
        }
        PermitManager.Permit permit = pm.acquirePermit();
        pm.disallowObtainPermits(permit);
        pm.releasePermit(permit);

        for (int i = 0; i < 10; i++) {
            permits.add(pm.acquirePermit());
        }
        for (int i = 0; i < 10; i++) {
            assertTrue(permits.get(i).isAllowed());
            pm.releasePermit(permits.get(i));
        }
    }

    @Test
    public void testLimitedPermitManager() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        PermitManager pm = new LimitedPermitManager(1, 0, TimeUnit.SECONDS, executorService);
        PermitManager.Permit permit1 = pm.acquirePermit();
        PermitManager.Permit permit2 = pm.acquirePermit();
        assertTrue(permit1.isAllowed());
        assertFalse(permit2.isAllowed());
        pm.releasePermit(permit2);
        PermitManager.Permit permit3 = pm.acquirePermit();
        assertFalse(permit3.isAllowed());
        pm.releasePermit(permit3);
        pm.releasePermit(permit1);
        PermitManager.Permit permit4 = pm.acquirePermit();
        assertTrue(permit4.isAllowed());
        pm.releasePermit(permit4);

        PermitManager pm2 = new LimitedPermitManager(2, 0, TimeUnit.SECONDS, executorService);

        PermitManager.Permit permit5 = pm2.acquirePermit();
        PermitManager.Permit permit6 = pm2.acquirePermit();
        assertTrue(permit5.isAllowed());
        assertTrue(permit6.isAllowed());
        assertTrue(pm2.disallowObtainPermits(permit5));
        assertFalse(pm2.disallowObtainPermits(permit6));
        pm2.releasePermit(permit5);
        pm2.releasePermit(permit6);
        PermitManager.Permit permit7 = pm2.acquirePermit();
        assertFalse(permit7.isAllowed());
        pm2.releasePermit(permit7);
        pm2.allowObtainPermits();
        PermitManager.Permit permit8 = pm2.acquirePermit();
        assertTrue(permit8.isAllowed());
        pm2.releasePermit(permit2);
    }
}
