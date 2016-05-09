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
package com.twitter.distributedlog.logsegment;

import com.twitter.distributedlog.util.Sizable;

public interface RollingPolicy {
    /**
     * Determines if a rollover may be appropriate at this time.
     *
     * @param sizable
     *          Any object that is sizable.
     * @param lastRolloverTimeMs
     *          last rolling time in millis.
     * @return true if a rollover is required. otherwise, false.
     */
    boolean shouldRollover(Sizable sizable, long lastRolloverTimeMs);
}
