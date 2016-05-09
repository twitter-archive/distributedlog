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

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.Future;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.runtime.AbstractFunction1;

public class CreateOp extends AbstractWriteOp {
  private final StreamManager streamManager;

  public CreateOp(String stream,
                  StatsLogger statsLogger,
                  StreamManager streamManager,
                  Long checksum,
                  Feature checksumEnabledFeature) {
    super(stream, requestStat(statsLogger, "create"), checksum, checksumEnabledFeature);
    this.streamManager = streamManager;
  }

  @Override
  protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                            Sequencer sequencer,
                                            Object txnLock) {
    Future<Void> result = streamManager.createStreamAsync(streamName());
    return result.map(new AbstractFunction1<Void, WriteResponse>() {
      @Override
      public WriteResponse apply(Void value) {
        return ResponseUtils.writeSuccess();
      }
    });
  }
}
