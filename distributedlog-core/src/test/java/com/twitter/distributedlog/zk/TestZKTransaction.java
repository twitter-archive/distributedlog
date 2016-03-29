package com.twitter.distributedlog.zk;

import com.twitter.distributedlog.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test Case for zookeeper transaction
 */
public class TestZKTransaction {

    static class CountDownZKOp extends ZKOp {

        final CountDownLatch commitLatch;
        final CountDownLatch abortLatch;

        CountDownZKOp(CountDownLatch commitLatch,
                      CountDownLatch abortLatch) {
            super(mock(Op.class));
            this.commitLatch = commitLatch;
            this.abortLatch = abortLatch;
        }

        @Override
        protected void commitOpResult(OpResult opResult) {
            this.commitLatch.countDown();
        }

        @Override
        protected void abortOpResult(Throwable t, @Nullable OpResult opResult) {
            this.abortLatch.countDown();
        }
    }

    @Test(timeout = 60000)
    public void testProcessNullResults() throws Exception {
        ZooKeeperClient zkc = mock(ZooKeeperClient.class);
        ZKTransaction transaction = new ZKTransaction(zkc);
        int numOps = 3;
        final CountDownLatch commitLatch = new CountDownLatch(numOps);
        final CountDownLatch abortLatch = new CountDownLatch(numOps);
        for (int i = 0; i < numOps; i++) {
            transaction.addOp(new CountDownZKOp(commitLatch, abortLatch));
        }
        transaction.processResult(
                KeeperException.Code.CONNECTIONLOSS.intValue(),
                "test-path",
                null,
                null);
        abortLatch.await();
        assertEquals(0, abortLatch.getCount());
        assertEquals(numOps, commitLatch.getCount());
    }

}
