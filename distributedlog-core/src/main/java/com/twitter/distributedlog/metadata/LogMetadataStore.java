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
package com.twitter.distributedlog.metadata;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.util.Future;

import java.net.URI;
import java.util.Iterator;

/**
 * Interface for log metadata store.
 */
@Beta
public interface LogMetadataStore {

    /**
     * Create a stream and return it is namespace location.
     *
     * @param logName
     *          name of the log
     * @return namespace location that stores this stream.
     */
    Future<URI> createLog(String logName);

    /**
     * Get the location of the log.
     *
     * @param logName
     *          name of the log
     * @return namespace location that stores this stream.
     */
    Future<Optional<URI>> getLogLocation(String logName);

    /**
     * Retrieves logs from the namespace.
     *
     * @return iterator of logs of the namespace.
     */
    Future<Iterator<String>> getLogs();

    /**
     * Register a namespace listener on streams changes.
     *
     * @param listener
     *          namespace listener
     */
    void registerNamespaceListener(NamespaceListener listener);
}
