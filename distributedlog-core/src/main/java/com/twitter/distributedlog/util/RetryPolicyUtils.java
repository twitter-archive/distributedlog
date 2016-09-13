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
package com.twitter.distributedlog.util;

import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;

/**
 * Utils for {@link org.apache.bookkeeper.zookeeper.RetryPolicy}
 */
public class RetryPolicyUtils {

    /**
     * Infinite retry policy
     */
    public static final RetryPolicy DEFAULT_INFINITE_RETRY_POLICY = infiniteRetry(200, 2000);

    /**
     * Create an infinite retry policy with backoff time between <i>baseBackOffTimeMs</i> and
     * <i>maxBackoffTimeMs</i>.
     *
     * @param baseBackoffTimeMs base backoff time in milliseconds
     * @param maxBackoffTimeMs maximum backoff time in milliseconds
     * @return an infinite retry policy
     */
    public static RetryPolicy infiniteRetry(long baseBackoffTimeMs, long maxBackoffTimeMs) {
        return new BoundExponentialBackoffRetryPolicy(baseBackoffTimeMs, maxBackoffTimeMs, Integer.MAX_VALUE);
    }

}
