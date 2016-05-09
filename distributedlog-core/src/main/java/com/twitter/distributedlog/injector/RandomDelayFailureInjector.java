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
package com.twitter.distributedlog.injector;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Injector that injects random delays
 */
public class RandomDelayFailureInjector implements FailureInjector {

    private static final Logger LOG = LoggerFactory.getLogger(RandomDelayFailureInjector.class);

    private final DynamicDistributedLogConfiguration dynConf;

    public RandomDelayFailureInjector(DynamicDistributedLogConfiguration dynConf) {
        this.dynConf = dynConf;
    }

    private int delayMs() {
        return dynConf.getEIInjectedWriteDelayMs();
    }

    private double delayPct() {
        return dynConf.getEIInjectedWriteDelayPercent();
    }

    private boolean enabled() {
        return delayMs() > 0 && delayPct() > 0;
    }

    @Override
    public void inject() {
        try {
            if (enabled() && Utils.randomPercent(delayPct())) {
                Thread.sleep(delayMs());
            }
        } catch (InterruptedException ex) {
            LOG.warn("delay was interrupted ", ex);
        }
    }
}
