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

import java.io.IOException;
import java.net.URI;

/**
 * Resolver to resolve the metadata used to instantiate a DL instance.
 *
 * <p>
 * E.g. we stored a common dl config under /messaging/distributedlog to use
 * bookkeeper cluster x. so all the distributedlog instances under this path
 * inherit this dl config. if a dl D is allocated under /messaging/distributedlog,
 * but use a different cluster y, so its metadata is stored /messaging/distributedlog/D.
 * The resolver resolve the URI
 * </p>
 *
 * <p>
 * The resolver looks up the uri path and tries to interpret the path segments from
 * bottom-to-top to see if there is a DL metadata bound. It stops when it found valid
 * dl metadata.
 * </p>
 */
public interface MetadataResolver {

    /**
     * Resolve the path to get the DL metadata.
     *
     * @param uri
     *          dl uri
     * @return dl metadata.
     * @throws IOException
     */
    public DLMetadata resolve(URI uri) throws IOException;
}
