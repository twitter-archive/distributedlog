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

import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.ClientInfo;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.distributedlog.thrift.service.HeartbeatOptions;
import com.twitter.distributedlog.thrift.service.ServerInfo;
import com.twitter.distributedlog.thrift.service.WriteContext;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Future;

import java.nio.ByteBuffer;
import java.util.List;

public class MockDistributedLogServices {

    static class MockBasicService implements DistributedLogService.ServiceIface {

        @Override
        public Future<ServerInfo> handshake() {
            return Future.value(new ServerInfo());
        }

        @Override
        public Future<ServerInfo> handshakeWithClientInfo(ClientInfo clientInfo) {
            return Future.value(new ServerInfo());
        }

        @Override
        public Future<WriteResponse> heartbeat(String stream, WriteContext ctx) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<WriteResponse> heartbeatWithOptions(String stream,
                                                          WriteContext ctx,
                                                          HeartbeatOptions options) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<WriteResponse> write(String stream,
                                           ByteBuffer data) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<WriteResponse> writeWithContext(String stream,
                                                      ByteBuffer data,
                                                      WriteContext ctx) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<BulkWriteResponse> writeBulkWithContext(String stream,
                                                              List<ByteBuffer> data,
                                                              WriteContext ctx) {
            return Future.value(new BulkWriteResponse());
        }

        @Override
        public Future<WriteResponse> truncate(String stream,
                                              String dlsn,
                                              WriteContext ctx) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<WriteResponse> release(String stream,
                                             WriteContext ctx) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<WriteResponse> create(String stream, WriteContext ctx) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<WriteResponse> delete(String stream,
                                            WriteContext ctx) {
            return Future.value(new WriteResponse());
        }

        @Override
        public Future<Void> setAcceptNewStream(boolean enabled) {
            return Future.value(null);
        }
    }

    public static class MockServerInfoService extends MockBasicService {

        protected ServerInfo serverInfo;

        public MockServerInfoService() {
            serverInfo = new ServerInfo();
        }

        public void updateServerInfo(ServerInfo serverInfo) {
            this.serverInfo = serverInfo;
        }

        @Override
        public Future<ServerInfo> handshake() {
            return Future.value(serverInfo);
        }

        @Override
        public Future<ServerInfo> handshakeWithClientInfo(ClientInfo clientInfo) {
            return Future.value(serverInfo);
        }
    }

}
