package com.twitter.distributedlog.rate;

public interface MovingAverageRate {
    double get();
    void add(int amount);
    void inc();
}
