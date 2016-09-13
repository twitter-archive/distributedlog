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
package com.twitter.distributedlog;

import com.twitter.distributedlog.util.RetryPolicyUtils;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * The zookeeper client builder used for testing.
 */
public class TestZooKeeperClientBuilder {

    /**
     * Return a zookeeper client builder for testing.
     *
     * @return a zookeeper client builder
     */
    public static ZooKeeperClientBuilder newBuilder() {
        return ZooKeeperClientBuilder.newBuilder()
                .retryPolicy(RetryPolicyUtils.DEFAULT_INFINITE_RETRY_POLICY)
                .connectionTimeoutMs(10000)
                .sessionTimeoutMs(60000)
                .zkAclId(null)
                .statsLogger(NullStatsLogger.INSTANCE);
    }

    /**
     * Create a zookeeper client builder with provided <i>conf</i> for testing.
     *
     * @param conf distributedlog configuration
     * @return zookeeper client builder
     */
    public static ZooKeeperClientBuilder newBuilder(DistributedLogConfiguration conf) {
        return ZooKeeperClientBuilder.newBuilder()
                .retryPolicy(RetryPolicyUtils.DEFAULT_INFINITE_RETRY_POLICY)
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .zkAclId(conf.getZkAclId())
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .requestRateLimit(conf.getZKRequestRateLimit())
                .statsLogger(NullStatsLogger.INSTANCE);
    }
}
