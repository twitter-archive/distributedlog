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

public class DeleteOp extends AbstractWriteOp {
    private final StreamManager streamManager;
    private final Counter deniedDeleteCounter;
    private final AccessControlManager accessControlManager;

    public DeleteOp(String stream,
                    StatsLogger statsLogger,
                    StatsLogger perStreamStatsLogger,
                    StreamManager streamManager,
                    Long checksum,
                    Feature checksumEnabledFeature,
                    AccessControlManager accessControlManager) {
        super(stream, requestStat(statsLogger, "delete"), checksum, checksumEnabledFeature);
        StreamOpStats streamOpStats = new StreamOpStats(statsLogger, perStreamStatsLogger);
        this.deniedDeleteCounter = streamOpStats.requestDeniedCounter("delete");
        this.accessControlManager = accessControlManager;
        this.streamManager = streamManager;
    }

    @Override
    protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                              Sequencer sequencer,
                                              Object txnLock) {
        Future<Void> result = streamManager.deleteAndRemoveAsync(streamName());
        return result.map(new AbstractFunction1<Void, WriteResponse>() {
            @Override
            public WriteResponse apply(Void value) {
                return ResponseUtils.writeSuccess();
            }
        });
    }

    @Override
    public void preExecute() throws DLException {
        if (!accessControlManager.allowTruncate(stream)) {
            deniedDeleteCounter.inc();
            throw new RequestDeniedException(stream, "delete");
        }
        super.preExecute();
    }
}
