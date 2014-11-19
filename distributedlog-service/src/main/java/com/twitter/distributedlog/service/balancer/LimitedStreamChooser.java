package com.twitter.distributedlog.service.balancer;

public class LimitedStreamChooser implements StreamChooser {

    public static LimitedStreamChooser of(StreamChooser underlying, int limit) {
        return new LimitedStreamChooser(underlying, limit);
    }

    final StreamChooser underlying;
    int limit;

    LimitedStreamChooser(StreamChooser underlying, int limit) {
        this.underlying = underlying;
        this.limit = limit;
    }

    @Override
    public synchronized String choose() {
        if (limit <= 0) {
            return null;
        }
        String s = underlying.choose();
        if (s == null) {
            limit = 0;
            return null;
        }
        --limit;
        return s;
    }
}
