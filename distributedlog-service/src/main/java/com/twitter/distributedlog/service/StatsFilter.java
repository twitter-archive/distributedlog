package com.twitter.distributedlog.service;

import com.google.common.base.Stopwatch;

import com.twitter.finagle.Service;
import com.twitter.finagle.SimpleFilter;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;

import java.util.concurrent.TimeUnit;

/**
 * Track distributedlog server finagle-service stats.
 */
class StatsFilter<Req, Rep> extends SimpleFilter<Req, Rep> {

    private final StatsLogger stats;
    private final Counter outstandingAsync;
    private final OpStatsLogger serviceExec;

    @Override
    public Future<Rep> apply(Req req, Service<Req, Rep> service) {
        Future<Rep> result = null;
        outstandingAsync.inc();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            result = service.apply(req);
            serviceExec.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
        } finally {
            outstandingAsync.dec();
            if (null == result) {
                serviceExec.registerFailedEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
        return result;
    }

    public StatsFilter(StatsLogger stats) {
        this.stats = stats;
        this.outstandingAsync = stats.getCounter("outstandingAsync");
        this.serviceExec = stats.getOpStatsLogger("serviceExec");
    }
}
