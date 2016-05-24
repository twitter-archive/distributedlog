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
package org.apache.bookkeeper.stats;

import com.codahale.metrics.health.HealthCheckRegistry;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extend the codahale metrics provider to run servlets
 */
public class CodahaleMetricsServletProvider extends CodahaleMetricsProvider {

    private final static Logger logger = LoggerFactory.getLogger(CodahaleMetricsServletProvider.class);

    ServletReporter servletReporter = null;
    private final HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

    @Override
    public void start(Configuration conf) {
        super.start(conf);
        Integer httpPort = conf.getInteger("codahaleStatsHttpPort", null);
        if (null != httpPort) {
            servletReporter = new ServletReporter(
                    getMetrics(),
                    healthCheckRegistry,
                    httpPort);
            try {
                servletReporter.start();
            } catch (Exception e) {
                logger.warn("Encountered error on starting the codahale metrics servlet", e);
            }
        }
    }

    @Override
    public void stop() {
        if (null != servletReporter) {
            try {
                servletReporter.stop();
            } catch (Exception e) {
                logger.error("Encountered error on stopping the codahale metrics servlet", e);
            }
        }
        super.stop();
    }
}
