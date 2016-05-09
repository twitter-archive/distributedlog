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
package com.twitter.distributedlog.service.config;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestServerConfiguration {

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testUnassignedShardId() {
        new ServerConfiguration().validate();
    }

    @Test(timeout = 60000)
    public void testAssignedShardId() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerShardId(100);
        conf.validate();
        assertEquals(100, conf.getServerShardId());
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testInvalidServerThreads() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerShardId(100);
        conf.setServerThreads(-1);
        conf.validate();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testInvalidDlsnVersion() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerShardId(100);
        conf.setDlsnVersion((byte) 9999);
        conf.validate();
    }

}
