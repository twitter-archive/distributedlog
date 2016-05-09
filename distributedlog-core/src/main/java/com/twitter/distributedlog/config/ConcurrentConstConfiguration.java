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
package com.twitter.distributedlog.config;

import com.google.common.base.Preconditions;

import org.apache.commons.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invariant thread-safe view of some configuration.
 */
public class ConcurrentConstConfiguration extends ConcurrentBaseConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(ConcurrentConstConfiguration.class);

    public ConcurrentConstConfiguration(Configuration conf) {
        Preconditions.checkNotNull(conf);
        copy(conf);
    }
}
