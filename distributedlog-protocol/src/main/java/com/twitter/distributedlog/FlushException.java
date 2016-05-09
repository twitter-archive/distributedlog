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

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

public class FlushException extends DLException {

    private final long lastTxIdWritten;
    private final long lastTxIdAcknowledged;

    private static final long serialVersionUID = -9060360360261130489L;

    public FlushException(String message, long lastTxIdWritten, long lastTxIdAcknowledged) {
        super(StatusCode.FLUSH_TIMEOUT, message);
        this.lastTxIdWritten = lastTxIdWritten;
        this.lastTxIdAcknowledged = lastTxIdAcknowledged;
    }

    public FlushException(String message, long lastTxIdWritten, long lastTxIdAcknowledged, Throwable cause) {
        super(StatusCode.FLUSH_TIMEOUT, message, cause);
        this.lastTxIdWritten = lastTxIdWritten;
        this.lastTxIdAcknowledged = lastTxIdAcknowledged;
    }

    public long getLastTxIdWritten() {
        return lastTxIdWritten;
    }

    public long getLastTxIdAcknowledged() {
        return lastTxIdAcknowledged;
    }
}
