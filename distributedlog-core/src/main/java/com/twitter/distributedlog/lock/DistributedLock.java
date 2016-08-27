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

import com.twitter.distributedlog.exceptions.LockingException;
import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.util.Future;

/**
 * Interface for distributed locking
 */
public interface DistributedLock extends AsyncCloseable {

    /**
     * Asynchronously acquire the lock.
     *
     * @return future represents the acquire result.
     */
    Future<? extends DistributedLock> asyncAcquire();

    /**
     * Check if hold lock. If it doesn't, then re-acquire the lock.
     *
     * @throws LockingException if the lock attempt fails
     * @see #checkOwnership()
     */
    void checkOwnershipAndReacquire() throws LockingException;

    /**
     * Check if the lock is held. If not, error out and do not re-acquire.
     * Use this in cases where there are many waiters by default and re-acquire
     * is unlikely to succeed.
     *
     * @throws LockingException if we lost the ownership
     * @see #checkOwnershipAndReacquire()
     */
    void checkOwnership() throws LockingException;

}
