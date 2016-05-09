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
package com.twitter.distributedlog.service.streamset;

import org.apache.commons.lang3.StringUtils;

/**
 * Stream Partition Converter
 */
public class DelimiterStreamPartitionConverter extends CacheableStreamPartitionConverter {

    private final String delimiter;

    public DelimiterStreamPartitionConverter() {
        this("_");
    }

    public DelimiterStreamPartitionConverter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    protected Partition newPartition(String streamName) {
        String[] parts = StringUtils.split(streamName, delimiter);
        if (null != parts && parts.length == 2) {
            try {
                int partition = Integer.parseInt(parts[1]);
                return new Partition(parts[0], partition);
            } catch (NumberFormatException nfe) {
                // ignore the exception
            }
        }
        return new Partition(streamName, 0);
    }
}
