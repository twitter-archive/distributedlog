package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.limiter.ChainedRequestLimiter;
import com.twitter.distributedlog.limiter.RequestLimiter;
import com.twitter.distributedlog.service.stream.StreamOpStats;
import com.twitter.distributedlog.service.stream.StreamOp;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;

public class StreamRequestLimiter extends DynamicRequestLimiter<StreamOp> {
    private final DynamicDistributedLogConfiguration dynConf;
    private final StatsLogger streamLogger;
    private final StatsLogger limiterStatLogger;
    private final String streamName;

    public StreamRequestLimiter(String streamName,
                                DynamicDistributedLogConfiguration dynConf,
                                StreamOpStats streamOpStats,
                                Feature disabledFeature) {

        super(dynConf, streamOpStats.baseScope("stream_limiter"), disabledFeature);
        this.limiterStatLogger = streamOpStats.baseScope("stream_limiter");
        this.dynConf = dynConf;
        this.streamName = streamName;
        this.streamLogger = streamOpStats.streamRequestStatsLogger(streamName);
        this.limiter = build();
    }

    @Override
    public RequestLimiter<StreamOp> build() {
        ChainedRequestLimiter.Builder<StreamOp> builder = new ChainedRequestLimiter.Builder<StreamOp>();
        builder.addLimiter(new RpsSoftLimiter(dynConf.getRpsSoftWriteLimit(), streamLogger));
        builder.addLimiter(new RpsHardLimiter(dynConf.getRpsHardWriteLimit(), streamLogger, streamName));
        builder.addLimiter(new BpsSoftLimiter(dynConf.getBpsSoftWriteLimit(), streamLogger));
        builder.addLimiter(new BpsHardLimiter(dynConf.getBpsHardWriteLimit(), streamLogger, streamName));
        builder.statsLogger(limiterStatLogger);
        return builder.build();
    }
}
