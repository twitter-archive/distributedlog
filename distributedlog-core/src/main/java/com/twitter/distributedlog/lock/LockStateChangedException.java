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

import com.twitter.distributedlog.LockingException;
import com.twitter.distributedlog.lock.ZKSessionLock.State;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Exception thrown when lock state changed
 */
public class LockStateChangedException extends LockingException {

    private static final long serialVersionUID = -3770866789942102262L;

    LockStateChangedException(String lockPath, Pair<String, Long> lockId,
                              State expectedState, State currentState) {
        super(lockPath, "Lock state of " + lockId + " for " + lockPath + " has changed : expected "
                + expectedState + ", but " + currentState);
    }
}
