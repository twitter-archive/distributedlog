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
package com.twitter.distributedlog.subscription;

import java.io.Closeable;

import scala.runtime.BoxedUnit;

import com.twitter.distributedlog.DLSN;
import com.twitter.util.Future;

public interface SubscriptionStateStore extends Closeable {
    /**
     * Get the last committed position stored for this subscription
     *
     * @return future represents the last commit position
     */
    public Future<DLSN> getLastCommitPosition();

    /**
     * Advances the position associated with the subscriber
     *
     * @param newPosition - new commit position
     * @return future represents the advance result
     */
    public Future<BoxedUnit> advanceCommitPosition(DLSN newPosition);
}
