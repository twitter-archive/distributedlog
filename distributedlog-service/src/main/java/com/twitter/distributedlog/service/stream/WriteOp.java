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
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.ProtocolUtils;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.RequestDeniedException;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Future;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.runtime.AbstractFunction1;

public class WriteOp extends AbstractWriteOp implements WriteOpWithPayload {
    static final Logger logger = LoggerFactory.getLogger(WriteOp.class);

    private final byte[] payload;
    private final boolean isRecordSet;

    // Stats
    private final Counter deniedWriteCounter;
    private final Counter successRecordCounter;
    private final Counter failureRecordCounter;
    private final Counter redirectRecordCounter;
    private final OpStatsLogger latencyStat;
    private final Counter bytes;
    private final Counter writeBytes;

    private final byte dlsnVersion;
    private final AccessControlManager accessControlManager;

    public WriteOp(String stream,
                   ByteBuffer data,
                   StatsLogger statsLogger,
                   StatsLogger perStreamStatsLogger,
                   ServerConfiguration conf,
                   byte dlsnVersion,
                   Long checksum,
                   boolean isRecordSet,
                   Feature checksumDisabledFeature,
                   AccessControlManager accessControlManager) {
        super(stream, requestStat(statsLogger, "write"), checksum, checksumDisabledFeature);
        payload = new byte[data.remaining()];
        data.get(payload);
        this.isRecordSet = isRecordSet;

        StreamOpStats streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);
        this.successRecordCounter = streamOpStats.recordsCounter("success");
        this.failureRecordCounter = streamOpStats.recordsCounter("failure");
        this.redirectRecordCounter = streamOpStats.recordsCounter("redirect");
        this.deniedWriteCounter = streamOpStats.requestDeniedCounter("write");
        this.writeBytes = streamOpStats.scopedRequestCounter("write", "bytes");
        this.latencyStat = streamOpStats.streamRequestLatencyStat(stream, "write");
        this.bytes = streamOpStats.streamRequestCounter(stream, "write", "bytes");

        this.dlsnVersion = dlsnVersion;
        this.accessControlManager = accessControlManager;

        final long size = getPayloadSize();
        result().addEventListener(new FutureEventListener<WriteResponse>() {
            @Override
            public void onSuccess(WriteResponse response) {
                if (response.getHeader().getCode() == StatusCode.SUCCESS) {
                    latencyStat.registerSuccessfulEvent(stopwatch().elapsed(TimeUnit.MICROSECONDS));
                    bytes.add(size);
                    writeBytes.add(size);
                } else {
                    latencyStat.registerFailedEvent(stopwatch().elapsed(TimeUnit.MICROSECONDS));
                }
            }
            @Override
            public void onFailure(Throwable cause) {
                latencyStat.registerFailedEvent(stopwatch().elapsed(TimeUnit.MICROSECONDS));
            }
        });
    }

    @Override
    public long getPayloadSize() {
      return payload.length;
    }

    @Override
    public Long computeChecksum() {
        return ProtocolUtils.writeOpCRC32(stream, payload);
    }

    @Override
    public void preExecute() throws DLException {
        if (!accessControlManager.allowWrite(stream)) {
            deniedWriteCounter.inc();
            throw new RequestDeniedException(stream, "write");
        }
        super.preExecute();
    }

    @Override
    protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                              Sequencer sequencer,
                                              Object txnLock) {
        if (!stream.equals(writer.getStreamName())) {
            logger.error("Write: Stream Name Mismatch in the Stream Map {}, {}", stream, writer.getStreamName());
            return Future.exception(new IllegalStateException("The stream mapping is incorrect, fail the request"));
        }

        long txnId;
        Future<DLSN> writeResult;
        synchronized (txnLock) {
            txnId = sequencer.nextId();
            LogRecord record = new LogRecord(txnId, payload);
            if (isRecordSet) {
                record.setRecordSet();
            }
            writeResult = writer.write(record);
        }
        return writeResult.map(new AbstractFunction1<DLSN, WriteResponse>() {
            @Override
            public WriteResponse apply(DLSN value) {
                successRecordCounter.inc();
                return ResponseUtils.writeSuccess().setDlsn(value.serialize(dlsnVersion));
            }
        });
    }

    @Override
    protected void fail(ResponseHeader header) {
        if (StatusCode.FOUND == header.getCode()) {
            redirectRecordCounter.inc();
        } else {
            failureRecordCounter.inc();
        }
        super.fail(header);
    }
}
