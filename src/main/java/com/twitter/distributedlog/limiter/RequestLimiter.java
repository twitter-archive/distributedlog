package com.twitter.distributedlog.limiter;

import com.twitter.distributedlog.exceptions.OverCapacityException;

public interface RequestLimiter<Request> {
    public void apply(Request request) throws OverCapacityException;
}
