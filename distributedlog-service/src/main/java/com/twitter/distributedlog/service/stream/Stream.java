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
package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.service.streamset.Partition;
import com.twitter.util.Future;
import java.io.IOException;

/**
 * Stream is the per stream request handler in the DL service layer. The collection of Streams in
 * the proxy are managed by StreamManager.
 */
public interface Stream {

    /**
     * Get the stream configuration for this stream
     *
     * @return stream configuration
     */
    DynamicDistributedLogConfiguration getStreamConfiguration();

    /**
     * Get the stream's last recorded current owner (may be out of date). Used
     * as a hint for the client.
     * @return last known owner for the stream
     */
    String getOwner();

    /**
     * Get the stream name.
     * @return stream name
     */
    String getStreamName();

    /**
     * Get the represented partition name.
     *
     * @return represented partition name.
     */
    Partition getPartition();

    /**
     * Expensive initialization code run after stream has been allocated in
     * StreamManager.
     *
     * @throws IOException when encountered exception on initialization
     */
    void initialize() throws IOException;

    /**
     * Another initialize method (actually Thread.start). Should probably be
     * moved to initiaize().
     */
    void start();

    /**
     * Asynchronous close method.
     * @param reason for closing
     * @return future satisfied once close complete
     */
    Future<Void> requestClose(String reason);

    /**
     * Delete the stream from DL backend.
     *
     * @throws IOException when encountered exception on deleting the stream.
     */
    void delete() throws IOException;

    /**
     * Execute the stream operation against this stream.
     *
     * @param op operation to execute
     */
    void submit(StreamOp op);
}
