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

import com.twitter.distributedlog.DLSN;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

import java.io.Closeable;
import java.util.Map;

/**
 * Store to manage subscriptions
 */
public interface SubscriptionsStore extends Closeable {

    /**
     * Get the last committed position stored for <i>subscriberId</i>.
     *
     * @param subscriberId
     *          subscriber id
     * @return future representing last committed position.
     */
    public Future<DLSN> getLastCommitPosition(String subscriberId);

    /**
     * Get the last committed positions for all subscribers.
     *
     * @return future representing last committed positions for all subscribers.
     */
    public Future<Map<String, DLSN>> getLastCommitPositions();

    /**
     * Advance the last committed position for <i>subscriberId</i>.
     *
     * @param subscriberId
     *          subscriber id.
     * @param newPosition
     *          new committed position.
     * @return future representing advancing result.
     */
    public Future<BoxedUnit> advanceCommitPosition(String subscriberId, DLSN newPosition);

}
