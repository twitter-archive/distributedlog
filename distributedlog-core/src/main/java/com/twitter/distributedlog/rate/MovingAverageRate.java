package com.twitter.distributedlog.rate;

public interface MovingAverageRate {
    double get();
    void add(long amount);
    void inc();
}
