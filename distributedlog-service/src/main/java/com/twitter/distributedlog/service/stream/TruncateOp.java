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
package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.ProtocolUtils;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.RequestDeniedException;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.runtime.AbstractFunction1;

public class TruncateOp extends AbstractWriteOp {

    static final Logger logger = LoggerFactory.getLogger(TruncateOp.class);

    private final Counter deniedTruncateCounter;
    private final DLSN dlsn;
    private final AccessControlManager accessControlManager;

    public TruncateOp(String stream,
                      DLSN dlsn,
                      StatsLogger statsLogger,
                      StatsLogger perStreamStatsLogger,
                      Long checksum,
                      Feature checksumDisabledFeature,
                      AccessControlManager accessControlManager) {
        super(stream, requestStat(statsLogger, "truncate"), checksum, checksumDisabledFeature);
        StreamOpStats streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);
        this.deniedTruncateCounter = streamOpStats.requestDeniedCounter("truncate");
        this.accessControlManager = accessControlManager;
        this.dlsn = dlsn;
    }

    @Override
    public Long computeChecksum() {
        return ProtocolUtils.truncateOpCRC32(stream, dlsn);
    }

    @Override
    protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                              Sequencer sequencer,
                                              Object txnLock) {
        if (!stream.equals(writer.getStreamName())) {
            logger.error("Truncate: Stream Name Mismatch in the Stream Map {}, {}", stream, writer.getStreamName());
            return Future.exception(new IllegalStateException("The stream mapping is incorrect, fail the request"));
        }
        return writer.truncate(dlsn).map(new AbstractFunction1<Boolean, WriteResponse>() {
            @Override
            public WriteResponse apply(Boolean v1) {
                return ResponseUtils.writeSuccess();
            }
        });
    }

    @Override
    public void preExecute() throws DLException {
        if (!accessControlManager.allowTruncate(stream)) {
            deniedTruncateCounter.inc();
            throw new RequestDeniedException(stream, "truncate");
        }
        super.preExecute();
    }
}
