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
package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.util.ProtocolUtils;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.util.Future;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.OpStatsLogger;

import scala.runtime.AbstractFunction1;

public abstract class AbstractWriteOp extends AbstractStreamOp<WriteResponse> {

    protected AbstractWriteOp(String stream,
                              OpStatsLogger statsLogger,
                              Long checksum,
                              Feature checksumDisabledFeature) {
        super(stream, statsLogger, checksum, checksumDisabledFeature);
    }

    @Override
    protected void fail(ResponseHeader header) {
        setResponse(ResponseUtils.write(header));
    }

    @Override
    public Long computeChecksum() {
        return ProtocolUtils.streamOpCRC32(stream);
    }

    @Override
    public Future<ResponseHeader> responseHeader() {
        return result().map(new AbstractFunction1<WriteResponse, ResponseHeader>() {
            @Override
            public ResponseHeader apply(WriteResponse response) {
                return response.getHeader();
            }
        });
    }
}
