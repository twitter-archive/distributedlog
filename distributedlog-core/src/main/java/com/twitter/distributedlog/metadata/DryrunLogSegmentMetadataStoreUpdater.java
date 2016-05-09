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
package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.util.Future;

public class DryrunLogSegmentMetadataStoreUpdater extends LogSegmentMetadataStoreUpdater {

    public DryrunLogSegmentMetadataStoreUpdater(DistributedLogConfiguration conf,
                                                LogSegmentMetadataStore metadataStore) {
        super(conf, metadataStore);
    }

    @Override
    public Transaction<Object> transaction() {
        return new Transaction<Object>() {
            @Override
            public void addOp(Op<Object> operation) {
                // no-op
            }

            @Override
            public Future<Void> execute() {
                return Future.Void();
            }

            @Override
            public void abort(Throwable reason) {
                // no-op
            }
        };
    }
}
