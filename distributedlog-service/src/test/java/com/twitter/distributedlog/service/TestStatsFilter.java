package com.twitter.distributedlog.service;

import com.twitter.finagle.Service;
import com.twitter.finagle.service.ConstantService;
import com.twitter.util.Await;
import com.twitter.util.Future;

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.NullStatsLogger;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestStatsFilter {

    class RuntimeExService<Req, Rep> extends Service<Req, Rep> {
        public Future<Rep> apply(Req request) {
            throw new RuntimeException("test");
        }
    }

    @Test(timeout = 60000)
    public void testServiceSuccess() throws Exception {
        StatsLogger stats = new NullStatsLogger();
        StatsFilter<String, String> filter = new StatsFilter<String, String>(stats);
        Future<String> result = filter.apply("", new ConstantService<String, String>(Future.value("result")));
        assertEquals("result", Await.result(result));
    }

    @Test(timeout = 60000)
    public void testServiceFailure() throws Exception {
        StatsLogger stats = new NullStatsLogger();
        StatsFilter<String, String> filter = new StatsFilter<String, String>(stats);
        try {
            filter.apply("", new RuntimeExService<String, String>());
        } catch (RuntimeException ex) {
        }
    }
}
