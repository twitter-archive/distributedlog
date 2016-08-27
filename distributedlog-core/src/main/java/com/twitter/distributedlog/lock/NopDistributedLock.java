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
import com.twitter.util.Future;

/**
 * An implementation of {@link DistributedLock} which does nothing.
 */
public class NopDistributedLock implements DistributedLock {

    public static final DistributedLock INSTANCE = new NopDistributedLock();

    private NopDistributedLock() {}

    @Override
    public Future<? extends DistributedLock> asyncAcquire() {
        return Future.value(this);
    }

    @Override
    public void checkOwnershipAndReacquire() throws LockingException {
        // no-op
    }

    @Override
    public void checkOwnership() throws LockingException {
        // no-op
    }

    @Override
    public Future<Void> asyncClose() {
        return Future.Void();
    }
}
