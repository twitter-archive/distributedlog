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
package com.twitter.distributedlog.function;

import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LogSegmentMetadata;
import scala.runtime.AbstractFunction1;

import java.util.List;

/**
 * Retrieve the last tx id from list of log segments
 */
public class GetLastTxIdFunction extends AbstractFunction1<List<LogSegmentMetadata>, Long> {

    public static final GetLastTxIdFunction INSTANCE = new GetLastTxIdFunction();

    private GetLastTxIdFunction() {}

    @Override
    public Long apply(List<LogSegmentMetadata> segmentList) {
        long lastTxId = DistributedLogConstants.INVALID_TXID;
        for (LogSegmentMetadata l : segmentList) {
            lastTxId = Math.max(lastTxId, l.getLastTxId());
        }
        return lastTxId;
    }
}
