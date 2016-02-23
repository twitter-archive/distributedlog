package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.exceptions.TooManyStreamsException;
import com.twitter.distributedlog.limiter.RequestLimiter;
import com.twitter.distributedlog.service.stream.StreamManager;
import com.twitter.distributedlog.service.stream.StreamOp;
import com.twitter.distributedlog.rate.MovingAverageRate;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

public class StreamAcquireLimiter implements RequestLimiter<StreamOp> {
    private final StreamManager streamManager;
    private final MovingAverageRate serviceRps;
    private final double serviceRpsLimit;
    private final Counter overlimitCounter;

    public StreamAcquireLimiter(StreamManager streamManager,
                                MovingAverageRate serviceRps,
                                double serviceRpsLimit,
                                StatsLogger statsLogger) {
        this.streamManager = streamManager;
        this.serviceRps = serviceRps;
        this.serviceRpsLimit = serviceRpsLimit;
        this.overlimitCounter = statsLogger.getCounter("overlimit");
    }

    @Override
    public void apply(StreamOp op) throws OverCapacityException {
        String streamName = op.streamName();
        if (serviceRpsLimit > -1 && serviceRps.get() > serviceRpsLimit && !streamManager.isAcquired(streamName)) {
            overlimitCounter.inc();
            throw new TooManyStreamsException("Request rate is too high to accept new stream " + streamName + ".");
        }
    }
}
