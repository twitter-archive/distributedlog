package com.twitter.distributedlog.zk;

import com.twitter.distributedlog.util.Transaction;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test Case for versioned set operation
 */
public class TestZKVersionedSetOp {

    @Test(timeout = 60000)
    public void testAbortNullOpResult() throws Exception {
        final AtomicReference<Throwable> exception =
                new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        ZKVersionedSetOp versionedSetOp =
                new ZKVersionedSetOp(mock(Op.class), new Transaction.OpListener<Version>() {
                    @Override
                    public void onCommit(Version r) {
                        // no-op
                    }

                    @Override
                    public void onAbort(Throwable t) {
                        exception.set(t);
                        latch.countDown();
                    }
                });
        KeeperException ke = KeeperException.create(KeeperException.Code.SESSIONEXPIRED);
        versionedSetOp.abortOpResult(ke, null);
        latch.await();
        assertTrue(ke == exception.get());
    }

    @Test(timeout = 60000)
    public void testAbortOpResult() throws Exception {
        final AtomicReference<Throwable> exception =
                new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        ZKVersionedSetOp versionedSetOp =
                new ZKVersionedSetOp(mock(Op.class), new Transaction.OpListener<Version>() {
                    @Override
                    public void onCommit(Version r) {
                        // no-op
                    }

                    @Override
                    public void onAbort(Throwable t) {
                        exception.set(t);
                        latch.countDown();
                    }
                });
        KeeperException ke = KeeperException.create(KeeperException.Code.SESSIONEXPIRED);
        OpResult opResult = new OpResult.ErrorResult(KeeperException.Code.NONODE.intValue());
        versionedSetOp.abortOpResult(ke, opResult);
        latch.await();
        assertTrue(exception.get() instanceof KeeperException.NoNodeException);
    }
}
