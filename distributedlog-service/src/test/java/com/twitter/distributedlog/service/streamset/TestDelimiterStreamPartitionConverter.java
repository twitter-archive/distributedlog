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

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Cases for {@link DelimiterStreamPartitionConverter}
 */
public class TestDelimiterStreamPartitionConverter {

    @Test(timeout = 20000)
    public void testNormalStream() throws Exception {
        StreamPartitionConverter converter = new DelimiterStreamPartitionConverter();
        assertEquals(new Partition("distributedlog-smoketest", 1),
                converter.convert("distributedlog-smoketest_1"));
        assertEquals(new Partition("distributedlog-smoketest-", 1),
                converter.convert("distributedlog-smoketest-_1"));
        assertEquals(new Partition("distributedlog-smoketest", 1),
                converter.convert("distributedlog-smoketest_000001"));
    }

    private void assertIdentify(String streamName, StreamPartitionConverter converter) {
        assertEquals(new Partition(streamName, 0), converter.convert(streamName));
    }

    @Test(timeout = 20000)
    public void testUnknownStream() throws Exception {
        StreamPartitionConverter converter = new DelimiterStreamPartitionConverter();
        assertIdentify("test1", converter);
        assertIdentify("test1-000001", converter);
        assertIdentify("test1_test1_000001", converter);
        assertIdentify("test1_test1", converter);
    }
}
