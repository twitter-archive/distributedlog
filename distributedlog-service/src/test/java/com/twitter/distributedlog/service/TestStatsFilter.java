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
package com.twitter.distributedlog.service;

import com.twitter.finagle.Service;
import com.twitter.finagle.service.ConstantService;
import com.twitter.util.Await;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.NullStatsLogger;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestStatsFilter {

    class RuntimeExService<Req, Rep> extends Service<Req, Rep> {
        public Future<Rep> apply(Req request) {
            throw new RuntimeException("test");
        }
    }

    @Test(timeout = 60000)
    public void testServiceSuccess() throws Exception {
        StatsLogger stats = new NullStatsLogger();
        StatsFilter<String, String> filter = new StatsFilter<String, String>(stats);
        Future<String> result = filter.apply("", new ConstantService<String, String>(Future.value("result")));
        assertEquals("result", Await.result(result));
    }

    @Test(timeout = 60000)
    public void testServiceFailure() throws Exception {
        StatsLogger stats = new NullStatsLogger();
        StatsFilter<String, String> filter = new StatsFilter<String, String>(stats);
        try {
            filter.apply("", new RuntimeExService<String, String>());
        } catch (RuntimeException ex) {
        }
    }
}
