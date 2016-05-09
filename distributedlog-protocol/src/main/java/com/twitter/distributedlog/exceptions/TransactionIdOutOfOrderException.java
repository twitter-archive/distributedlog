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
package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class TransactionIdOutOfOrderException extends DLException {

    private static final long serialVersionUID = -6239322552103630036L;
    // TODO: copied from DistributedLogConstants (we should think about how to separated common constants)
    public static final long INVALID_TXID = -999;
    private final long lastTxnId;

    public TransactionIdOutOfOrderException(long smallerTxnId, long lastTxnId) {
        super(StatusCode.TRANSACTION_OUT_OF_ORDER,
              "Received smaller txn id " + smallerTxnId + ", last txn id is " + lastTxnId);
        this.lastTxnId = lastTxnId;
    }

    public TransactionIdOutOfOrderException(long invalidTxnId) {
        super(StatusCode.TRANSACTION_OUT_OF_ORDER,
            "The txn id " + invalidTxnId + " is invalid and will break the sequence");
        lastTxnId = INVALID_TXID;
    }

    public long getLastTxnId() {
        return lastTxnId;
    }
}
