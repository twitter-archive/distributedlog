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

public class WriteCancelledException extends DLException {

    private static final long serialVersionUID = -1836146493496072122L;

    public WriteCancelledException(String stream, Throwable t) {
        super(StatusCode.WRITE_CANCELLED_EXCEPTION,
            "Write cancelled on stream " +
            stream + " due to an earlier error", t);
    }

    public WriteCancelledException(String stream, String reason) {
        super(StatusCode.WRITE_CANCELLED_EXCEPTION,
                "Write cancelled on stream " + stream + " due to : " + reason);
    }

    public WriteCancelledException(String stream) {
        super(StatusCode.WRITE_CANCELLED_EXCEPTION,
            "Write cancelled on stream " +
            stream + " due to an earlier error");
    }
}
