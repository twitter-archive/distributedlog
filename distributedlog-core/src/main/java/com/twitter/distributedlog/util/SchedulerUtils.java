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

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class SchedulerUtils {

    static final Logger logger = LoggerFactory.getLogger(SchedulerUtils.class);

    public static void shutdownScheduler(ExecutorService service, long timeout, TimeUnit timeUnit) {
        if (null == service) {
            return;
        }
        service.shutdown();
        try {
            service.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            logger.warn("Interrupted when shutting down scheduler : ", e);
        }
        service.shutdownNow();
    }

    public static void shutdownScheduler(OrderedSafeExecutor service, long timeout, TimeUnit timeUnit) {
        if (null == service) {
            return;
        }
        service.shutdown();
        try {
            service.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            logger.warn("Interrupted when shutting down scheduler : ", e);
        }
        service.forceShutdown(timeout, timeUnit);
    }
}
