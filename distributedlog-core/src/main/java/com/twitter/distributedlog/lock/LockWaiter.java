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

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lock waiter represents the attempt that application tries to lock.
 */
public class LockWaiter {

    private static final Logger logger = LoggerFactory.getLogger(LockWaiter.class);

    private final String lockId;
    private final String currentOwner;
    private final Future<Boolean> acquireFuture;

    public LockWaiter(String lockId,
                      String currentOwner,
                      Future<Boolean> acquireFuture) {
        this.lockId = lockId;
        this.currentOwner = currentOwner;
        this.acquireFuture = acquireFuture;
    }

    /**
     * Return the lock id of the waiter.
     *
     * @return lock id of the waiter
     */
    public String getId() {
        return lockId;
    }

    /**
     * Return the owner that observed when waiter is waiting.
     *
     * @return the owner that observed when waiter is waiting
     */
    public String getCurrentOwner() {
        return currentOwner;
    }

    /**
     * Return the future representing the waiting result.
     *
     * <p>If the future is interrupted (e.g. {@link Future#within(Duration, Timer)}),
     * the waiter will automatically clean up its waiting state.
     *
     * @return the future representing the acquire result.
     */
    public Future<Boolean> getAcquireFuture() {
        return acquireFuture;
    }

    /**
     * Wait for the acquire result.
     *
     * @return true if acquired successfully, otherwise false.
     */
    public boolean waitForAcquireQuietly() {
        boolean success = false;
        try {
            success = Await.result(acquireFuture);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (LockTimeoutException lte) {
            logger.debug("Timeout on lock acquiring", lte);
        } catch (Exception e) {
            logger.error("Caught exception waiting for lock acquired", e);
        }
        return success;
    }

}
