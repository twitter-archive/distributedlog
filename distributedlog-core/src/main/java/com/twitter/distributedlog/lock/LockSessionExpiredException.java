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
import com.twitter.distributedlog.lock.ZKSessionLock.State;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Exception indicates that the lock's zookeeper session was expired before the lock request could complete.
 */
public class LockSessionExpiredException extends LockingException {

    private static final long serialVersionUID = 8775253025963470331L;

    public LockSessionExpiredException(String lockPath, Pair<String, Long> lockId, State currentState) {
        super(lockPath, "lock at path " + lockPath + " with id " + lockId + " expired early in state : " + currentState);
    }
}
