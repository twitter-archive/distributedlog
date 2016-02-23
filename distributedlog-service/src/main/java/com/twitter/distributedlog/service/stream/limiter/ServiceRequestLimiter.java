package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.ChainedRequestLimiter;
import com.twitter.distributedlog.limiter.ComposableRequestLimiter.OverlimitFunction;
import com.twitter.distributedlog.limiter.RequestLimiter;
import com.twitter.distributedlog.service.stream.StreamManager;
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
    private final MovingAverageRate serviceBps;
    private final StreamManager streamManager;

    public ServiceRequestLimiter(DynamicDistributedLogConfiguration dynConf,
                                 StatsLogger statsLogger,
                                 MovingAverageRate serviceRps,
                                 MovingAverageRate serviceBps,
                                 StreamManager streamManager,
                                 Feature disabledFeature) {
        super(dynConf, statsLogger, disabledFeature);
        this.limiterStatLogger = statsLogger;
        this.streamManager = streamManager;
        this.serviceRps = serviceRps;
        this.serviceBps = serviceBps;
        this.limiter = build();
    }

    @Override
    public RequestLimiter<StreamOp> build() {
        int rpsStreamAcquireLimit = dynConf.getRpsStreamAcquireServiceLimit();
        int rpsSoftServiceLimit = dynConf.getRpsSoftServiceLimit();
        int rpsHardServiceLimit = dynConf.getRpsHardServiceLimit();
        int bpsStreamAcquireLimit = dynConf.getBpsStreamAcquireServiceLimit();
        int bpsSoftServiceLimit = dynConf.getBpsSoftServiceLimit();
        int bpsHardServiceLimit = dynConf.getBpsHardServiceLimit();

        RequestLimiterBuilder rpsHardLimiterBuilder = RequestLimiterBuilder.newRpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("rps_hard_limit"))
            .limit(rpsHardServiceLimit)
            .overlimit(new OverlimitFunction<StreamOp>() {
                @Override
                public void apply(StreamOp request) throws OverCapacityException {
                    throw new OverCapacityException("RPS limit exceeded for the service instance");
                }
            });

        RequestLimiterBuilder rpsSoftLimiterBuilder = RequestLimiterBuilder.newRpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("rps_soft_limit"))
            .limit(rpsSoftServiceLimit);

        RequestLimiterBuilder bpsHardLimiterBuilder = RequestLimiterBuilder.newBpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("bps_hard_limit"))
            .limit(bpsHardServiceLimit)
            .overlimit(new OverlimitFunction<StreamOp>() {
                @Override
                public void apply(StreamOp request) throws OverCapacityException {
                    throw new OverCapacityException("BPS limit exceeded for the service instance");
                }
            });

        RequestLimiterBuilder bpsSoftLimiterBuilder = RequestLimiterBuilder.newBpsLimiterBuilder()
            .statsLogger(limiterStatLogger.scope("bps_soft_limit"))
            .limit(bpsSoftServiceLimit);

        ChainedRequestLimiter.Builder<StreamOp> builder = new ChainedRequestLimiter.Builder<StreamOp>();
        builder.addLimiter(new StreamAcquireLimiter(streamManager, serviceRps, rpsStreamAcquireLimit, limiterStatLogger.scope("rps_acquire")));
        builder.addLimiter(new StreamAcquireLimiter(streamManager, serviceBps, bpsStreamAcquireLimit, limiterStatLogger.scope("bps_acquire")));
        builder.addLimiter(bpsHardLimiterBuilder.build());
        builder.addLimiter(bpsSoftLimiterBuilder.build());
        builder.addLimiter(rpsHardLimiterBuilder.build());
        builder.addLimiter(rpsSoftLimiterBuilder.build());
        builder.statsLogger(limiterStatLogger);
        return builder.build();
    }
}
