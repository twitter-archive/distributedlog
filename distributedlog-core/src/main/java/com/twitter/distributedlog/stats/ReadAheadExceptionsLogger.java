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
package com.twitter.distributedlog.stats;

import org.apache.bookkeeper.stats.StatsLogger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stats logger to log exceptions happened in {@link com.twitter.distributedlog.readahead.ReadAheadWorker}.
 * They are counters of exceptions happened on each read ahead phase:
 * <code>`scope`/exceptions/`phase`/`code`</code>. `scope` is the current scope of
 * stats logger, `phase` is the read ahead phase, while `code` is the exception code. Check
 * {@link com.twitter.distributedlog.readahead.ReadAheadPhase} for details about phases and
 * {@link BKExceptionStatsLogger} for details about `code`.
 */
public class ReadAheadExceptionsLogger {

    private final StatsLogger statsLogger;
    private StatsLogger parentExceptionStatsLogger;
    private final ConcurrentMap<String, BKExceptionStatsLogger> exceptionStatsLoggers =
            new ConcurrentHashMap<String, BKExceptionStatsLogger>();

    public ReadAheadExceptionsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
    }

    public BKExceptionStatsLogger getBKExceptionStatsLogger(String phase) {
        // initialize the parent exception stats logger lazily
        if (null == parentExceptionStatsLogger) {
            parentExceptionStatsLogger = statsLogger.scope("exceptions");
        }
        BKExceptionStatsLogger exceptionStatsLogger = exceptionStatsLoggers.get(phase);
        if (null == exceptionStatsLogger) {
            exceptionStatsLogger = new BKExceptionStatsLogger(parentExceptionStatsLogger.scope(phase));
            BKExceptionStatsLogger oldExceptionStatsLogger =
                    exceptionStatsLoggers.putIfAbsent(phase, exceptionStatsLogger);
            if (null != oldExceptionStatsLogger) {
                exceptionStatsLogger = oldExceptionStatsLogger;
            }
        }
        return exceptionStatsLogger;
    }
}
