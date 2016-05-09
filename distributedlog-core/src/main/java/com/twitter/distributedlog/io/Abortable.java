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

import java.io.IOException;

/**
 * An {@code Abortable} is a source or destination of data that can be aborted.
 * The abort method is invoked to release resources that the object is holding
 * (such as open files). The abort happens when the object is in an error state,
 * which it couldn't be closed gracefully.
 *
 * @see java.io.Closeable
 * @since 0.3.32
 */
public interface Abortable {

    /**
     * Aborts the object and releases any resources associated with it.
     * If the object is already aborted then invoking this method has no
     * effect.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void abort() throws IOException;
}
