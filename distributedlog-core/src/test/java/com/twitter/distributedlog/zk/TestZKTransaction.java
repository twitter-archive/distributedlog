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

import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
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

    @Test(timeout = 60000)
    public void testAbortTransaction() throws Exception {
        ZooKeeperClient zkc = mock(ZooKeeperClient.class);
        ZKTransaction transaction = new ZKTransaction(zkc);
        int numOps = 3;
        final CountDownLatch commitLatch = new CountDownLatch(numOps);
        final CountDownLatch abortLatch = new CountDownLatch(numOps);
        for (int i = 0; i < numOps; i++) {
            transaction.addOp(new CountDownZKOp(commitLatch, abortLatch));
        }
        transaction.abort(new DLIllegalStateException("Illegal State"));
        abortLatch.await();
        assertEquals(0, abortLatch.getCount());
        assertEquals(numOps, commitLatch.getCount());
    }

}
