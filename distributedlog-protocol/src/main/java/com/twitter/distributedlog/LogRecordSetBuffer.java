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

import java.nio.ByteBuffer;

/**
 * Write representation of a {@link LogRecordSet}.
 * It is a buffer of log record set, used for transmission.
 */
public interface LogRecordSetBuffer {

    /**
     * Return number of records in current record set.
     *
     * @return number of records in current record set.
     */
    int getNumRecords();

    /**
     * Return number of bytes in current record set.
     *
     * @return number of bytes in current record set.
     */
    int getNumBytes();

    /**
     * Get the buffer to transmit.
     *
     * @return the buffer to transmit.
     */
    ByteBuffer getBuffer();

    /**
     * Complete transmit.
     *
     * @param lssn log segment sequence number
     * @param entryId entry id
     * @param startSlotId start slot id
     */
    void completeTransmit(long lssn, long entryId, long startSlotId);

    /**
     * Abort transmit.
     *
     * @param reason reason to abort.
     */
    void abortTransmit(Throwable reason);

}
