package com.twitter.distributedlog.limiter;

import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRequestLimiter {

    class MockRequest {
    }

    class MockRequestLimiter implements RequestLimiter<MockRequest> {
        int count;
        MockRequestLimiter() {
            this.count = 0;
        }
        public void apply(MockRequest request) {
            count++;
        }
        public int getCount() {
            return count;
        }
    }

    @Test
    public void testChainedRequestLimiter() throws Exception {
        MockRequestLimiter limiter1 = new MockRequestLimiter();
        MockRequestLimiter limiter2 = new MockRequestLimiter();
        ChainedRequestLimiter.Builder<MockRequest> limiterBuilder =
                new ChainedRequestLimiter.Builder<MockRequest>();
        limiterBuilder.addLimiter(limiter1)
                      .addLimiter(limiter2);
        ChainedRequestLimiter<MockRequest> limiter = limiterBuilder.build();
        assertEquals(0, limiter1.getCount());
        assertEquals(0, limiter2.getCount());
        limiter.apply(new MockRequest());
        assertEquals(1, limiter1.getCount());
        assertEquals(1, limiter2.getCount());
        limiter.apply(new MockRequest());
        assertEquals(2, limiter1.getCount());
        assertEquals(2, limiter2.getCount());
    }
}
