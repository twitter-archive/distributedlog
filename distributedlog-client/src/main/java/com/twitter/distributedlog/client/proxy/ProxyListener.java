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
package com.twitter.distributedlog.client.proxy;

import com.twitter.distributedlog.thrift.service.ServerInfo;

import java.net.SocketAddress;

/**
 * Listener on server changes
 */
public interface ProxyListener {
    /**
     * When a proxy's server info changed, it would be notified.
     *
     * @param address
     *          proxy address
     * @param client
     *          proxy client that executes handshaking
     * @param serverInfo
     *          proxy's server info
     */
    void onHandshakeSuccess(SocketAddress address, ProxyClient client, ServerInfo serverInfo);

    /**
     * Failed to handshake with a proxy.
     *
     * @param address
     *          proxy address
     * @param client
     *          proxy client
     * @param cause
     *          failure reason
     */
    void onHandshakeFailure(SocketAddress address, ProxyClient client, Throwable cause);
}
