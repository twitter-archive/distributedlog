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
package com.twitter.distributedlog.io;

import com.twitter.distributedlog.DLSN;

/**
 * Listener on transmit results.
 */
public interface TransmitListener {

    /**
     * Finalize the transmit result and result the last
     * {@link DLSN} in this transmit.
     *
     * @param lssn
     *          log segment sequence number
     * @param entryId
     *          entry id
     * @return last dlsn in this transmit
     */
    DLSN finalizeTransmit(long lssn, long entryId);

    /**
     * Complete the whole transmit.
     *
     * @param lssn
     *          log segment sequence number
     * @param entryId
     *          entry id
     */
    void completeTransmit(long lssn, long entryId);

    /**
     * Abort the transmit.
     *
     * @param reason
     *          reason to abort transmit
     */
    void abortTransmit(Throwable reason);
}
