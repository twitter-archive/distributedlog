package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.limiter.ChainedRequestLimiter;
import com.twitter.distributedlog.limiter.RequestLimiter;
import com.twitter.distributedlog.service.stream.StreamManager;
import com.twitter.distributedlog.service.stream.StreamOpStats;
import com.twitter.distributedlog.service.stream.StreamOp;
import com.twitter.distributedlog.rate.MovingAverageRate;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Request limiter for the service instance (global request limiter).
 */
public class ServiceRequestLimiter extends DynamicRequestLimiter<StreamOp> {
    private final StatsLogger limiterStatLogger;
    private final MovingAverageRate serviceRps;
    private final StreamManager streamManager;

    public ServiceRequestLimiter(DynamicDistributedLogConfiguration dynConf,
                                 StreamOpStats streamOpStats,
                                 MovingAverageRate serviceRps,
                                 StreamManager streamManager,
                                 Feature disabledFeature) {
        super(dynConf, streamOpStats.baseScope("service_limiter"), disabledFeature);
        this.limiterStatLogger = streamOpStats.baseScope("service_limiter");
        limiterStatLogger.scope("test");
        this.streamManager = streamManager;
        this.serviceRps = serviceRps;
        this.limiter = build();
    }

    @Override
    public RequestLimiter<StreamOp> build() {
        int rpsStreamAcquireLimit = dynConf.getRpsStreamAcquireServiceLimit();

        ChainedRequestLimiter.Builder<StreamOp> builder = new ChainedRequestLimiter.Builder<StreamOp>();
        builder.addLimiter(new StreamAcquireLimiter(streamManager, serviceRps, rpsStreamAcquireLimit));
        builder.statsLogger(limiterStatLogger);
        return builder.build();
    }
}
