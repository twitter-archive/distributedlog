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

public class LockingException extends DLException {

    private static final long serialVersionUID = -4960278188448464473L;

    public LockingException(String lockPath, String message) {
        this(StatusCode.LOCKING_EXCEPTION, lockPath, message);
    }

    public LockingException(String lockPath, String message, Throwable cause) {
        this(StatusCode.LOCKING_EXCEPTION, lockPath, message, cause);
    }

    protected LockingException(StatusCode code, String lockPath, String message) {
        super(code, String.format("LockPath - %s: %s", lockPath, message));
    }

    protected LockingException(StatusCode code, String lockPath, String message, Throwable cause) {
        super(code, String.format("LockPath - %s: %s", lockPath, message), cause);
    }
}
