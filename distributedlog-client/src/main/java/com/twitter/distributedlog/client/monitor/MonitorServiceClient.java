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
package com.twitter.distributedlog.client.monitor;

import com.twitter.util.Future;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

public interface MonitorServiceClient {

    /**
     * Check a given stream.
     *
     * @param stream
     *          stream.
     * @return check result.
     */
    Future<Void> check(String stream);

    /**
     * Send heartbeat to the stream and its readers
     *
     * @param stream
     *          stream.
     * @return check result.
     */
    Future<Void> heartbeat(String stream);

    /**
     * Get current ownership distribution from current monitor service view.
     *
     * @return current ownership distribution
     */
    Map<SocketAddress, Set<String>> getStreamOwnershipDistribution();

    /**
     * Enable/Disable accepting new stream on a given proxy
     *
     * @param enabled
     *          flag to enable/disable accepting new streams on a given proxy
     * @return void
     */
    Future<Void> setAcceptNewStream(boolean enabled);

    /**
     * Close the client
     */
    void close();
}
