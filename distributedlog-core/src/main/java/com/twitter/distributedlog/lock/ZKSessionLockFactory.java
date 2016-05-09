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
package com.twitter.distributedlog.lock;

import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory to create zookeeper based locks.
 */
public class ZKSessionLockFactory implements SessionLockFactory {

    private final ZooKeeperClient zkc;
    private final String clientId;
    private final OrderedScheduler lockStateExecutor;
    private final long lockOpTimeout;
    private final int lockCreationRetries;
    private final long zkRetryBackoffMs;

    // Stats
    private final StatsLogger lockStatsLogger;

    public ZKSessionLockFactory(ZooKeeperClient zkc,
                                String clientId,
                                OrderedScheduler lockStateExecutor,
                                int lockCreationRetries,
                                long lockOpTimeout,
                                long zkRetryBackoffMs,
                                StatsLogger statsLogger) {
        this.zkc = zkc;
        this.clientId = clientId;
        this.lockStateExecutor = lockStateExecutor;
        this.lockCreationRetries = lockCreationRetries;
        this.lockOpTimeout = lockOpTimeout;
        this.zkRetryBackoffMs = zkRetryBackoffMs;

        this.lockStatsLogger = statsLogger.scope("lock");
    }

    @Override
    public Future<SessionLock> createLock(String lockPath,
                                          DistributedLockContext context) {
        AtomicInteger numRetries = new AtomicInteger(lockCreationRetries);
        final AtomicReference<Throwable> interruptedException = new AtomicReference<Throwable>(null);
        Promise<SessionLock> createPromise =
                new Promise<SessionLock>(new com.twitter.util.Function<Throwable, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Throwable t) {
                interruptedException.set(t);
                return BoxedUnit.UNIT;
            }
        });
        createLock(
                lockPath,
                context,
                interruptedException,
                numRetries,
                createPromise,
                0L);
        return createPromise;
    }

    void createLock(final String lockPath,
                    final DistributedLockContext context,
                    final AtomicReference<Throwable> interruptedException,
                    final AtomicInteger numRetries,
                    final Promise<SessionLock> createPromise,
                    final long delayMs) {
        lockStateExecutor.schedule(lockPath, new Runnable() {
            @Override
            public void run() {
                if (null != interruptedException.get()) {
                    createPromise.updateIfEmpty(new Throw<SessionLock>(interruptedException.get()));
                    return;
                }
                try {
                    SessionLock lock = new ZKSessionLock(
                            zkc,
                            lockPath,
                            clientId,
                            lockStateExecutor,
                            lockOpTimeout,
                            lockStatsLogger,
                            context);
                    createPromise.updateIfEmpty(new Return<SessionLock>(lock));
                } catch (DLInterruptedException dlie) {
                    // if the creation is interrupted, throw the exception without retrie.
                    createPromise.updateIfEmpty(new Throw<SessionLock>(dlie));
                    return;
                } catch (IOException e) {
                    if (numRetries.getAndDecrement() < 0) {
                        createPromise.updateIfEmpty(new Throw<SessionLock>(e));
                        return;
                    }
                    createLock(
                            lockPath,
                            context,
                            interruptedException,
                            numRetries,
                            createPromise,
                            zkRetryBackoffMs);
                }
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }
}
