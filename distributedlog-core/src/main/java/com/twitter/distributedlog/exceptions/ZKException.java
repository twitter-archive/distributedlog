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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * TODO: move ZKException to distributedlog-protocol
 */
public class ZKException extends DLException {

    private static final long serialVersionUID = 7542748595054923600L;

    final KeeperException.Code code;

    public ZKException(String msg, Code code) {
        super(StatusCode.ZOOKEEPER_ERROR, msg + " : " + code);
        this.code = code;
    }

    public ZKException(String msg, KeeperException exception) {
        super(StatusCode.ZOOKEEPER_ERROR, msg, exception);
        this.code = exception.code();
    }

    public Code getKeeperExceptionCode() {
        return this.code;
    }

    public static boolean isRetryableZKException(ZKException zke) {
        KeeperException.Code code = zke.getKeeperExceptionCode();
        return KeeperException.Code.CONNECTIONLOSS == code ||
                KeeperException.Code.OPERATIONTIMEOUT == code ||
                KeeperException.Code.SESSIONEXPIRED == code ||
                KeeperException.Code.SESSIONMOVED == code;
    }
}
