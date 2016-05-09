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

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.util.PermitLimiter;

public class WriteLimiter {

    String streamName;
    final PermitLimiter streamLimiter;
    final PermitLimiter globalLimiter;

    public WriteLimiter(String streamName, PermitLimiter streamLimiter, PermitLimiter globalLimiter) {
        this.streamName = streamName;
        this.streamLimiter = streamLimiter;
        this.globalLimiter = globalLimiter;
    }

    public void acquire() throws OverCapacityException {
        if (!streamLimiter.acquire()) {
            throw new OverCapacityException(String.format("Stream write capacity exceeded for stream %s", streamName));
        }
        try {
            if (!globalLimiter.acquire()) {
                throw new OverCapacityException("Global write capacity exceeded");
            }
        } catch (OverCapacityException ex) {
            streamLimiter.release(1);
            throw ex;
        }
    }

    public void release() {
        release(1);
    }

    public void release(int permits) {
        streamLimiter.release(permits);
        globalLimiter.release(permits);
    }
}
