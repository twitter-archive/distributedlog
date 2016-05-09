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

import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.HashMap;
import java.util.Map;

/**
 * A Util to logger stats on bk exceptions.
 */
public class BKExceptionStatsLogger {

    public static String getMessage(int code) {
        switch (code) {
            case Code.OK:
                return "OK";
            case Code.ReadException:
                return "ReadException";
            case Code.QuorumException:
                return "QuorumException";
            case Code.NoBookieAvailableException:
                return "NoBookieAvailableException";
            case Code.DigestNotInitializedException:
                return "DigestNotInitializedException";
            case Code.DigestMatchException:
                return "DigestMatchException";
            case Code.NotEnoughBookiesException:
                return "NotEnoughBookiesException";
            case Code.NoSuchLedgerExistsException:
                return "NoSuchLedgerExistsException";
            case Code.BookieHandleNotAvailableException:
                return "BookieHandleNotAvailableException";
            case Code.ZKException:
                return "ZKException";
            case Code.LedgerRecoveryException:
                return "LedgerRecoveryException";
            case Code.LedgerClosedException:
                return "LedgerClosedException";
            case Code.WriteException:
                return "WriteException";
            case Code.NoSuchEntryException:
                return "NoSuchEntryException";
            case Code.IncorrectParameterException:
                return "IncorrectParameterException";
            case Code.InterruptedException:
                return "InterruptedException";
            case Code.ProtocolVersionException:
                return "ProtocolVersionException";
            case Code.MetadataVersionException:
                return "MetadataVersionException";
            case Code.LedgerFencedException:
                return "LedgerFencedException";
            case Code.UnauthorizedAccessException:
                return "UnauthorizedAccessException";
            case Code.UnclosedFragmentException:
                return "UnclosedFragmentException";
            case Code.WriteOnReadOnlyBookieException:
                return "WriteOnReadOnlyBookieException";
            case Code.IllegalOpException:
                return "IllegalOpException";
            default:
                return "UnexpectedException";
        }
    }

    private final StatsLogger parentLogger;
    private final Map<Integer, Counter> exceptionCounters;

    public BKExceptionStatsLogger(StatsLogger parentLogger) {
        this.parentLogger = parentLogger;
        this.exceptionCounters = new HashMap<Integer, Counter>();
    }

    public Counter getExceptionCounter(int rc) {
        Counter counter = exceptionCounters.get(rc);
        if (null != counter) {
            return counter;
        }
        // TODO: it would be better to have BKException.Code.get(rc)
        synchronized (exceptionCounters) {
            counter = exceptionCounters.get(rc);
            if (null != counter) {
                return counter;
            }
            counter = parentLogger.getCounter(getMessage(rc));
            exceptionCounters.put(rc, counter);
        }
        return counter;
    }
}
