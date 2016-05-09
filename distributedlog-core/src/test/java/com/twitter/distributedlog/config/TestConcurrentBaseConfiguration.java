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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestConcurrentBaseConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(TestConcurrentBaseConfiguration.class);

    @Test
    public void testBasicOperations() throws Exception {
        ConcurrentBaseConfiguration conf = new ConcurrentBaseConfiguration();
        conf.setProperty("prop1", "1");
        assertEquals(1, conf.getInt("prop1"));
        conf.setProperty("prop1", "2");
        assertEquals(2, conf.getInt("prop1"));
        conf.clearProperty("prop1");
        assertEquals(null, conf.getInteger("prop1", null));
        conf.setProperty("prop1", "1");
        conf.setProperty("prop2", "2");
        assertEquals(1, conf.getInt("prop1"));
        assertEquals(2, conf.getInt("prop2"));
        conf.clearProperty("prop1");
        assertEquals(null, conf.getInteger("prop1", null));
        assertEquals(2, conf.getInt("prop2"));
    }
}
