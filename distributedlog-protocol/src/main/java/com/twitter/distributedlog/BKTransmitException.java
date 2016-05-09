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

/**
 * Thrown when the send to bookkeeper fails
 * This is thrown by the next attempt to write, send or flush
 */
public class BKTransmitException extends DLException {

    private static final long serialVersionUID = -5796100450432076091L;

    final int bkRc;

    public BKTransmitException(String message, int bkRc) {
        super(StatusCode.BK_TRANSMIT_ERROR, message + " : " + bkRc);
        this.bkRc = bkRc;
    }

    public int getBKResultCode() {
        return this.bkRc;
    }

}
