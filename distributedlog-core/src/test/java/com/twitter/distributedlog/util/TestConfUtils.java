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
package com.twitter.distributedlog.util;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestConfUtils {

    @Test
    public void testLoadConfiguration() {
        Configuration conf1 = new CompositeConfiguration();
        conf1.setProperty("key1", "value1");
        conf1.setProperty("key2", "value2");
        conf1.setProperty("key3", "value3");

        Configuration conf2 = new CompositeConfiguration();
        conf2.setProperty("bkc.key1", "bkc.value1");
        conf2.setProperty("bkc.key4", "bkc.value4");

        assertEquals("value1", conf1.getString("key1"));
        assertEquals("value2", conf1.getString("key2"));
        assertEquals("value3", conf1.getString("key3"));
        assertEquals(null, conf1.getString("key4"));

        ConfUtils.loadConfiguration(conf1, conf2, "bkc.");

        assertEquals("bkc.value1", conf1.getString("key1"));
        assertEquals("value2", conf1.getString("key2"));
        assertEquals("value3", conf1.getString("key3"));
        assertEquals("bkc.value4", conf1.getString("key4"));
        assertEquals(null, conf1.getString("bkc.key1"));
        assertEquals(null, conf1.getString("bkc.key4"));
    }
}
