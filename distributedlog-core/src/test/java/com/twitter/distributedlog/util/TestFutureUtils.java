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

import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * Test Case for {@link FutureUtils}
 */
public class TestFutureUtils {

    static class TestException extends IOException {
    }

    @Test(timeout = 60000)
    public void testWithin() throws Exception {
        OrderedScheduler scheduler = OrderedScheduler.newBuilder()
                .corePoolSize(1)
                .name("test-within")
                .build();
        final Promise<Void> promiseToTimeout = new Promise<Void>();
        final Promise<Void> finalPromise = new Promise<Void>();
        FutureUtils.within(
                promiseToTimeout,
                10,
                TimeUnit.MILLISECONDS,
                new TestException(),
                scheduler,
                "test-within"
        ).addEventListener(new FutureEventListener<Void>() {
            @Override
            public void onFailure(Throwable cause) {
                FutureUtils.setException(finalPromise, cause);
            }

            @Override
            public void onSuccess(Void value) {
                FutureUtils.setValue(finalPromise, value);
            }
        });
        try {
            FutureUtils.result(finalPromise);
            fail("Should fail with TestException");
        } catch (TestException te) {
            // expected
        }
    }

}
