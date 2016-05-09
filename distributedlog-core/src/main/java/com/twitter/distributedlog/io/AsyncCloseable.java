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

import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Function;
import com.twitter.util.Future;

/**
 * A {@code AsyncCloseable} is a source or destination of data that can be closed asynchronously.
 * The close method is invoked to release resources that the object is
 * holding (such as open files).
 */
public interface AsyncCloseable {

    Function<AsyncCloseable, Future<Void>> CLOSE_FUNC = new Function<AsyncCloseable, Future<Void>>() {
        @Override
        public Future<Void> apply(AsyncCloseable closeable) {
            return closeable.asyncClose();
        }
    };

    Function<AsyncCloseable, Future<Void>> CLOSE_FUNC_IGNORE_ERRORS = new Function<AsyncCloseable, Future<Void>>() {
        @Override
        public Future<Void> apply(AsyncCloseable closeable) {
            return FutureUtils.ignore(closeable.asyncClose());
        }
    };

    AsyncCloseable NULL = new AsyncCloseable() {
        @Override
        public Future<Void> asyncClose() {
            return Future.Void();
        }
    };

    /**
     * Closes this source and releases any system resources associated
     * with it. If the source is already closed then invoking this
     * method has no effect.
     *
     * @return future representing the close result.
     */
    Future<Void> asyncClose();
}
