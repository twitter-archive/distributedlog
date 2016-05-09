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

import com.google.common.base.Preconditions;

public class BitMaskUtils {

    /**
     * 1) Unset all bits where value in mask is set.
     * 2) Set these bits to value specified by newValue.
     *
     * e.g.
     * if oldValue = 1010, mask = 0011, newValue = 0001
     * 1) 1010 -> 1000
     * 2) 1000 -> 1001
     *
     * @param oldValue expected old value
     * @param mask the mask of the value for updates
     * @param newValue new value to set
     * @return updated value
     */
    public static long set(long oldValue, long mask, long newValue) {
        Preconditions.checkArgument(oldValue >= 0L && mask >= 0L && newValue >= 0L);
        return ((oldValue & (~mask)) | (newValue & mask));
    }

    /**
     * Get the bits where mask is 1
     *
     * @param value value
     * @param mask mask of the value
     * @return the bit of the mask
     */
    public static long get(long value, long mask) {
        Preconditions.checkArgument(value >= 0L && mask >= 0L);
        return (value & mask);
    }
}
