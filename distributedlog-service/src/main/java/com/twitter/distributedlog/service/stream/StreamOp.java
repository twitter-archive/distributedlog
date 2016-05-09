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

import com.google.common.base.Stopwatch;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.Future;

import java.nio.ByteBuffer;

/**
 * An operation applied to a stream.
 */
public interface StreamOp {
    /**
     * Execute a stream op with the supplied writer.
     *
     * @param writer active writer for applying the change
     * @param sequencer sequencer used for generating transaction id for stream operations
     * @param txnLock transaction lock to guarantee ordering of transaction id
     * @return a future satisfied when the operation completes execution
     */
    Future<Void> execute(AsyncLogWriter writer,
                         Sequencer sequencer,
                         Object txnLock);

    /**
     * Invoked before the stream op is executed.
     */
    void preExecute() throws DLException;

    /**
     * Return the response header (containing the status code etc.).
     *
     * @return A future containing the response header or the exception
     *      encountered by the op if it failed.
     */
    Future<ResponseHeader> responseHeader();

    /**
     * Abort the operation with the givem exception.
     */
    void fail(Throwable t);

    /**
     * Return the stream name.
     */
    String streamName();

    /**
     * Stopwatch gives the start time of the operation.
     */
    Stopwatch stopwatch();

    /**
     * Compute checksum from arguments.
     */
    Long computeChecksum();
}
