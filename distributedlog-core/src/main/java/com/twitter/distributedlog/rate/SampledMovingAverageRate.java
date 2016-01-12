package com.twitter.distributedlog.rate;

import com.twitter.common.stats.Rate;
import com.twitter.util.TimerTask;
import com.twitter.util.Timer;
import com.twitter.util.Time;
import java.util.concurrent.atomic.AtomicLong;

class SampledMovingAverageRate implements MovingAverageRate {
    private final Rate rate;
    private final AtomicLong total;

    private double value;

    public SampledMovingAverageRate(int intervalSecs) {
        this.total = new AtomicLong(0);
        this.rate = Rate.of("Ignore", total)
            .withWindowSize(intervalSecs)
            .build();
        this.value = 0;
    }

    @Override
    public double get() {
        return value;
    }

    @Override
    public void add(long amount) {
        total.getAndAdd(amount);
    }

    @Override
    public void inc() {
        add(1);
    }

    void sample() {
        value = rate.doSample();
    }
}
