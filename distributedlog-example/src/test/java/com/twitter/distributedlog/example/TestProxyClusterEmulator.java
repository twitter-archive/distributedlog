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
package com.twitter.distributedlog.example;

import java.net.BindException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestProxyClusterEmulator {
    static final Logger logger = LoggerFactory.getLogger(TestProxyClusterEmulator.class);

    @Test(timeout = 60000)
    public void testStartup() throws Exception {
        final ProxyClusterEmulator emulator = new ProxyClusterEmulator(new String[] {"-port", "8000"});
        final AtomicBoolean failed = new AtomicBoolean(false);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    started.countDown();
                    emulator.start();
                } catch (BindException ex) {
                } catch (InterruptedException ex) {
                } catch (Exception ex) {
                    failed.set(true);
                } finally {
                    finished.countDown();
                }
            }
        });
        thread.start();
        started.await();
        Thread.sleep(1000);
        thread.interrupt();
        finished.await();
        emulator.stop();
        assert(!failed.get());
    }
}
