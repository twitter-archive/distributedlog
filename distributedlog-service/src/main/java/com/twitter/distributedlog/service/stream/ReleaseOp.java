package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.AsyncLogWriter;
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

import scala.runtime.AbstractFunction1;

public class ReleaseOp extends AbstractWriteOp {
    private final StreamManager streamManager;
    private final Counter deniedReleaseCounter;
    private final AccessControlManager accessControlManager;

    public ReleaseOp(String stream,
                     StatsLogger statsLogger,
                     StatsLogger perStreamStatsLogger,
                     StreamManager streamManager,
                     Long checksum,
                     Feature checksumDisabledFeature,
                     AccessControlManager accessControlManager) {
        super(stream, requestStat(statsLogger, "release"), checksum, checksumDisabledFeature);
        StreamOpStats streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);
        this.deniedReleaseCounter = streamOpStats.requestDeniedCounter("release");
        this.accessControlManager = accessControlManager;
        this.streamManager = streamManager;
    }

    @Override
    protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                              Sequencer sequencer,
                                              Object txnLock) {
        Future<Void> result = streamManager.closeAndRemoveAsync(streamName());
        return result.map(new AbstractFunction1<Void, WriteResponse>() {
            @Override
            public WriteResponse apply(Void value) {
                return ResponseUtils.writeSuccess();
            }
        });
    }

    @Override
    public void preExecute() throws DLException {
        if (!accessControlManager.allowRelease(stream)) {
            deniedReleaseCounter.inc();
            throw new RequestDeniedException(stream, "release");
        }
        super.preExecute();
    }
}
