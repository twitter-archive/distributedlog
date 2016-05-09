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
package com.twitter.distributedlog.acl;

/**
 * Access Control on stream operations
 */
public interface AccessControlManager {

    /**
     * Whether allowing writing to a stream.
     *
     * @param stream
     *          Stream to write
     * @return true if allowing writing to the given stream, otherwise false.
     */
    boolean allowWrite(String stream);

    /**
     * Whether allowing truncating a given stream.
     *
     * @param stream
     *          Stream to truncate
     * @return true if allowing truncating a given stream.
     */
    boolean allowTruncate(String stream);

    /**
     * Whether allowing deleting a given stream.
     *
     * @param stream
     *          Stream to delete
     * @return true if allowing deleting a given stream.
     */
    boolean allowDelete(String stream);

    /**
     * Whether allowing proxies to acquire a given stream.
     *
     * @param stream
     *          stream to acquire
     * @return true if allowing proxies to acquire the given stream.
     */
    boolean allowAcquire(String stream);

    /**
     * Whether allowing proxies to release ownership for a given stream.
     *
     * @param stream
     *          stream to release
     * @return true if allowing proxies to release a given stream.
     */
    boolean allowRelease(String stream);

    /**
     * Close the access control manager.
     */
    void close();
}
