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

import com.google.common.annotations.Beta;
import com.twitter.distributedlog.Entry;
import com.twitter.distributedlog.util.Sizable;
import org.apache.bookkeeper.client.AsyncCallback;

/**
 * An interface class to write the enveloped entry (serialized bytes of
 * {@link Entry} into the log segment.
 *
 * <p>It is typically used by {@link LogSegmentWriter}.
 *
 * @see LogSegmentWriter
 *
 * TODO: The interface is leveraging bookkeeper's callback and status code now
 *       Consider making it more generic.
 */
@Beta
public interface LogSegmentEntryWriter extends Sizable {

    /**
     * Get the log segment id.
     *
     * @return log segment id.
     */
    long getLogSegmentId();

    /**
     * Close the entry writer.
     */
    void asyncClose(AsyncCallback.CloseCallback callback, Object ctx);

    /**
     * Async add entry to the log segment.
     * <p>The implementation semantic follows
     * {@link org.apache.bookkeeper.client.LedgerHandle#asyncAddEntry(
     * byte[], int, int, AsyncCallback.AddCallback, Object)}
     *
     * @param data
     *          data to add
     * @param offset
     *          offset in the data
     * @param length
     *          length of the data
     * @param callback
     *          callback
     * @param ctx
     *          ctx
     * @see org.apache.bookkeeper.client.LedgerHandle#asyncAddEntry(
     * byte[], int, int, AsyncCallback.AddCallback, Object)
     */
    void asyncAddEntry(byte[] data, int offset, int length,
                       AsyncCallback.AddCallback callback, Object ctx);
}
