package com.twitter.distributedlog.benchmark.utils;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper over rate limiter
 */
public class ShiftableRateLimiter implements Runnable {

    private final RateLimiter rateLimiter;
    private final ScheduledExecutorService executor;
    private final double initialRate, maxRate, changeRate;
    private double nextRate;

    public ShiftableRateLimiter(double initialRate,
                                double maxRate,
                                double changeRate,
                                long changeInterval,
                                TimeUnit changeIntervalUnit) {
        this.initialRate = initialRate;
        this.maxRate = maxRate;
        this.changeRate = changeRate;
        this.nextRate = initialRate;
        this.rateLimiter = RateLimiter.create(initialRate);
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.executor.scheduleAtFixedRate(this, changeInterval, changeInterval, changeIntervalUnit);
    }

    @Override
    public void run() {
        this.nextRate = Math.min(nextRate + changeRate, maxRate);
        this.rateLimiter.setRate(nextRate);
    }

    public RateLimiter getLimiter() {
        return this.rateLimiter;
    }
}
