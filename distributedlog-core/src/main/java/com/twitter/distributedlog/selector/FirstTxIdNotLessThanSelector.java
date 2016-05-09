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
package com.twitter.distributedlog.selector;

import com.twitter.distributedlog.LogRecordWithDLSN;

/**
 * Save the first record with transaction id not less than the provided transaction id.
 * If all records' transaction id is less than provided transaction id, save the last record.
 */
public class FirstTxIdNotLessThanSelector implements LogRecordSelector {

    LogRecordWithDLSN result;
    final long txId;
    boolean found = false;

    public FirstTxIdNotLessThanSelector(long txId) {
        this.txId = txId;
    }

    @Override
    public void process(LogRecordWithDLSN record) {
        if (found) {
            return;
        }
        this.result = record;
        if (record.getTransactionId() >= txId) {
            found = true;
        }
    }

    @Override
    public LogRecordWithDLSN result() {
        return this.result;
    }
}
