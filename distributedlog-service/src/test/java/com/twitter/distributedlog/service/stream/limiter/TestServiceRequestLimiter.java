package com.twitter.distributedlog.service.stream.limiter;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.ConcurrentConstConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.limiter.RateLimiter;
import com.twitter.distributedlog.limiter.AbstractRequestLimiter;
import com.twitter.distributedlog.limiter.ChainedRequestLimiter;
import com.twitter.distributedlog.limiter.RequestLimiter;

import java.util.concurrent.atomic.AtomicInteger;

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

        public static class Builder extends RateLimiter.Builder {
            @Override
            public RateLimiter build() {
                return new CounterLimiter(limit);
            }
        }

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

    class MockServiceRequestLimiter extends AbstractRequestLimiter<MockRequest> {
        int limitHitCount;
        int limit;

        MockServiceRequestLimiter(int limit) {
            super(limit);
            this.limit = limit;
        }

        @Override
        protected int cost(MockRequest request) {
            return request.getSize();
        }

        @Override
        protected void overlimit() throws OverCapacityException {
            limitHitCount++;
        }

        public int getLimitHitCount() {
            return limitHitCount;
        }
    }

    class MockHardRequestLimiter extends MockServiceRequestLimiter {

        MockHardRequestLimiter(int limit) {
            super(limit);
        }

        @Override
        protected void overlimit() throws OverCapacityException {
            super.overlimit();
            throw new OverCapacityException("Limit exceeded");
        }
    }

    class MockSoftRequestLimiter extends MockServiceRequestLimiter {

        MockSoftRequestLimiter(int limit) {
            super(limit);
        }
    }

    @Test(timeout = 60000)
    public void testDynamicLimiter() throws Exception {
        final AtomicInteger id = new AtomicInteger(0);
        final DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(
                new ConcurrentConstConfiguration(new DistributedLogConfiguration()));
        DynamicRequestLimiter<MockRequest> limiter = new DynamicRequestLimiter<MockRequest>(dynConf, NullStatsLogger.INSTANCE) {
            @Override
            public RequestLimiter<MockRequest> build() {
                id.getAndIncrement();
                return new MockRequestLimiter();
            }
        };
        assertEquals(1, id.get());
        dynConf.setProperty("test1", 1);
        assertEquals(2, id.get());
        dynConf.setProperty("test2", 2);
        assertEquals(3, id.get());
    }

    @Test(timeout = 60000)
    public void testDynamicLimiterWithException() throws Exception {
        final AtomicInteger id = new AtomicInteger(0);
        final DynamicDistributedLogConfiguration dynConf = new DynamicDistributedLogConfiguration(
                new ConcurrentConstConfiguration(new DistributedLogConfiguration()));
        DynamicRequestLimiter<MockRequest> limiter = new DynamicRequestLimiter<MockRequest>(dynConf, NullStatsLogger.INSTANCE) {
            @Override
            public RequestLimiter<MockRequest> build() {
                if (id.incrementAndGet() >= 2) {
                    throw new RuntimeException("exception in dynamic limiter build()");
                }
                return new MockRequestLimiter();
            }
        };
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
        MockHardRequestLimiter limiter = new MockHardRequestLimiter(1);
        limiter.setRateLimiter(new CounterLimiter.Builder());
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
        MockSoftRequestLimiter softLimiter = new MockSoftRequestLimiter(1);
        softLimiter.setRateLimiter(new CounterLimiter.Builder());
        MockHardRequestLimiter hardLimiter = new MockHardRequestLimiter(3);
        hardLimiter.setRateLimiter(new CounterLimiter.Builder());

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
