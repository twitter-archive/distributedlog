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
package com.twitter.distributedlog.limiter;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestRequestLimiter {

    class MockRequest {
    }

    class MockRequestLimiter implements RequestLimiter<MockRequest> {
        int count;
        MockRequestLimiter() {
            this.count = 0;
        }
        public void apply(MockRequest request) {
            count++;
        }
        public int getCount() {
            return count;
        }
    }

    @Test
    public void testChainedRequestLimiter() throws Exception {
        MockRequestLimiter limiter1 = new MockRequestLimiter();
        MockRequestLimiter limiter2 = new MockRequestLimiter();
        ChainedRequestLimiter.Builder<MockRequest> limiterBuilder =
                new ChainedRequestLimiter.Builder<MockRequest>();
        limiterBuilder.addLimiter(limiter1)
                      .addLimiter(limiter2);
        ChainedRequestLimiter<MockRequest> limiter = limiterBuilder.build();
        assertEquals(0, limiter1.getCount());
        assertEquals(0, limiter2.getCount());
        limiter.apply(new MockRequest());
        assertEquals(1, limiter1.getCount());
        assertEquals(1, limiter2.getCount());
        limiter.apply(new MockRequest());
        assertEquals(2, limiter1.getCount());
        assertEquals(2, limiter2.getCount());
    }
}
