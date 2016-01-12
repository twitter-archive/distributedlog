package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.ComposableRequestLimiter.CostFunction;
import com.twitter.distributedlog.limiter.ComposableRequestLimiter.OverlimitFunction;
import com.twitter.distributedlog.limiter.ComposableRequestLimiter;
import com.twitter.distributedlog.limiter.ChainedRequestLimiter;
import com.twitter.distributedlog.limiter.GuavaRateLimiter;
import com.twitter.distributedlog.limiter.RateLimiter;
import com.twitter.distributedlog.limiter.RequestLimiter;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestServiceRequestLimiter {
    static final Logger LOG = LoggerFactory.getLogger(TestServiceRequestLimiter.class);

    class MockRequest {
        int size;
        MockRequest() {
            this(1);
        }
        MockRequest(int size) {
            this.size = size;
        }
        int getSize() {
            return size;
        }
    }

    class MockRequestLimiter implements RequestLimiter<MockRequest> {
        public void apply(MockRequest request) {
        }
    }

    static class CounterLimiter implements RateLimiter {
        final int limit;
        int count;

        public CounterLimiter(int limit) {
            this.limit = limit;
            this.count = 0;
        }

        @Override
        public boolean acquire(int permits) {
            if (++count > limit) {
                return false;
            }
            return true;
        }
    }

    class MockHardRequestLimiter implements RequestLimiter<MockRequest> {

        RequestLimiter<MockRequest> limiter;
        int limitHitCount;

        MockHardRequestLimiter(int limit) {
            this(GuavaRateLimiter.of(limit));
        }

        MockHardRequestLimiter(RateLimiter limiter) {
            this.limiter = new ComposableRequestLimiter<MockRequest>(
                limiter,
                new OverlimitFunction<MockRequest>() {
                    public void apply(MockRequest request) throws OverCapacityException {
                        limitHitCount++;
                        throw new OverCapacityException("Limit exceeded");
                    }
                },
                new CostFunction<MockRequest>() {
                    public int apply(MockRequest request) {
                        return request.getSize();
                    }
                });
        }

        @Override
        public void apply(MockRequest op) throws OverCapacityException {
            limiter.apply(op);
        }

        public int getLimitHitCount() {
            return limitHitCount;
        }
    }

    class MockSoftRequestLimiter implements RequestLimiter<MockRequest> {

        RequestLimiter<MockRequest> limiter;
        int limitHitCount;

        MockSoftRequestLimiter(int limit) {
            this(GuavaRateLimiter.of(limit));
        }

        MockSoftRequestLimiter(RateLimiter limiter) {
            this.limiter = new ComposableRequestLimiter<MockRequest>(
                limiter,
                new OverlimitFunction<MockRequest>() {
                    public void apply(MockRequest request) throws OverCapacityException {
                        limitHitCount++;
                    }
                },
                new CostFunction<MockRequest>() {
                    public int apply(MockRequest request) {
                        return request.getSize();
                    }
                });
        }

        @Override
        public void apply(MockRequest op) throws OverCapacityException {
            limiter.apply(op);
        }

        public int getLimitHitCount() {
            return limitHitCount;
        }
    }

    @Test(timeout = 60000)
    public void testDynamicLimiter() throws Exception {
        final AtomicInteger id = new AtomicInteger(0);
        final DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(
                new ConcurrentConstConfiguration(new DistributedLogConfiguration()));
        DynamicRequestLimiter<MockRequest> limiter = new DynamicRequestLimiter<MockRequest>(
                dynConf, NullStatsLogger.INSTANCE, new SettableFeature("", 0)) {
            @Override
            public RequestLimiter<MockRequest> build() {
                id.getAndIncrement();
                return new MockRequestLimiter();
            }
        };
        limiter.initialize();
        assertEquals(1, id.get());
        dynConf.setProperty("test1", 1);
        assertEquals(2, id.get());
        dynConf.setProperty("test2", 2);
        assertEquals(3, id.get());
    }

    @Test(timeout = 60000)
    public void testDynamicLimiterWithDisabledFeature() throws Exception {
        final DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(
                new ConcurrentConstConfiguration(new DistributedLogConfiguration()));
        final MockSoftRequestLimiter rateLimiter = new MockSoftRequestLimiter(0);
        final SettableFeature disabledFeature = new SettableFeature("", 0);
        DynamicRequestLimiter<MockRequest> limiter = new DynamicRequestLimiter<MockRequest>(
                dynConf, NullStatsLogger.INSTANCE, disabledFeature) {
            @Override
            public RequestLimiter<MockRequest> build() {
                return rateLimiter;
            }
        };
        limiter.initialize();
        assertEquals(0, rateLimiter.getLimitHitCount());

        // Not disabled, rate limiter was invoked
        limiter.apply(new MockRequest(Integer.MAX_VALUE));
        assertEquals(1, rateLimiter.getLimitHitCount());

        // Disabled, rate limiter not invoked
        disabledFeature.set(1);
        limiter.apply(new MockRequest(Integer.MAX_VALUE));
        assertEquals(1, rateLimiter.getLimitHitCount());
    }

    @Test(timeout = 60000)
    public void testDynamicLimiterWithException() throws Exception {
        final AtomicInteger id = new AtomicInteger(0);
        final DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(
                new ConcurrentConstConfiguration(new DistributedLogConfiguration()));
        DynamicRequestLimiter<MockRequest> limiter = new DynamicRequestLimiter<MockRequest>(
                dynConf, NullStatsLogger.INSTANCE, new SettableFeature("", 0)) {
            @Override
            public RequestLimiter<MockRequest> build() {
                if (id.incrementAndGet() >= 2) {
                    throw new RuntimeException("exception in dynamic limiter build()");
                }
                return new MockRequestLimiter();
            }
        };
        limiter.initialize();
        assertEquals(1, id.get());
        try {
            dynConf.setProperty("test1", 1);
            fail("should have thrown on config failure");
        } catch (RuntimeException ex) {
        }
        assertEquals(2, id.get());
    }

    @Test(timeout = 60000)
    public void testServiceRequestLimiter() throws Exception {
        MockHardRequestLimiter limiter = new MockHardRequestLimiter(new CounterLimiter(1));
        limiter.apply(new MockRequest());
        try {
            limiter.apply(new MockRequest());
        } catch (OverCapacityException ex) {
        }
        assertEquals(1, limiter.getLimitHitCount());
    }

    @Test(timeout = 60000)
    public void testServiceRequestLimiterWithDefaultRate() throws Exception {
        MockHardRequestLimiter limiter = new MockHardRequestLimiter(-1);
        limiter.apply(new MockRequest(Integer.MAX_VALUE));
        limiter.apply(new MockRequest(Integer.MAX_VALUE));
        assertEquals(0, limiter.getLimitHitCount());
    }

    @Test(timeout = 60000)
    public void testServiceRequestLimiterWithZeroRate() throws Exception {
        MockHardRequestLimiter limiter = new MockHardRequestLimiter(0);
        try {
            limiter.apply(new MockRequest(1));
            fail("should have failed with overcap");
        } catch (OverCapacityException ex) {
        }
        assertEquals(1, limiter.getLimitHitCount());
    }

    @Test(timeout = 60000)
    public void testChainedServiceRequestLimiter() throws Exception {
        MockSoftRequestLimiter softLimiter = new MockSoftRequestLimiter(new CounterLimiter(1));
        MockHardRequestLimiter hardLimiter = new MockHardRequestLimiter(new CounterLimiter(3));

        RequestLimiter<MockRequest> limiter =
                new ChainedRequestLimiter.Builder<MockRequest>()
                .addLimiter(softLimiter)
                .addLimiter(hardLimiter)
                .build();

        assertEquals(0, softLimiter.getLimitHitCount());
        assertEquals(0, hardLimiter.getLimitHitCount());

        limiter.apply(new MockRequest());
        assertEquals(0, softLimiter.getLimitHitCount());
        assertEquals(0, hardLimiter.getLimitHitCount());

        limiter.apply(new MockRequest());
        assertEquals(1, softLimiter.getLimitHitCount());
        assertEquals(0, hardLimiter.getLimitHitCount());

        limiter.apply(new MockRequest());
        assertEquals(2, softLimiter.getLimitHitCount());
        assertEquals(0, hardLimiter.getLimitHitCount());

        try {
            limiter.apply(new MockRequest());
        } catch (OverCapacityException ex) {
        }
        assertEquals(3, softLimiter.getLimitHitCount());
        assertEquals(1, hardLimiter.getLimitHitCount());
    }
}
