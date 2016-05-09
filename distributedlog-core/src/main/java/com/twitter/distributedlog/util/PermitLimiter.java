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
package com.twitter.distributedlog.util;

/**
 * A simple limiter interface which tracks acquire/release of permits, for
 * example for tracking outstanding writes.
 */
public interface PermitLimiter {

    public static PermitLimiter NULL_PERMIT_LIMITER = new PermitLimiter() {
        @Override
        public boolean acquire() {
            return true;
        }
        @Override
        public void release(int permits) {
        }
    };

    /**
     * Acquire a permit.
     *
     * @return true if successfully acquire a permit, otherwise false.
     */
    boolean acquire();

    /**
     * Release a permit.
     */
    void release(int permits);
}
