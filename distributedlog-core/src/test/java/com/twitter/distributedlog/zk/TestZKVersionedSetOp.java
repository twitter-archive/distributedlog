/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
