package com.twitter.distributedlog.logsegment;

import com.twitter.distributedlog.util.Sizable;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Case for {@link RollingPolicy}s.
 */
public class TestRollingPolicy {

    static class TestSizable implements Sizable {

        long size;

        TestSizable(long size) {
            this.size = size;
        }

        @Override
        public long size() {
            return size;
        }
    }

    @Test(timeout = 60000)
    public void testTimeBasedRollingPolicy() {
        TimeBasedRollingPolicy policy1 = new TimeBasedRollingPolicy(Long.MAX_VALUE);
        TestSizable maxSize = new TestSizable(Long.MAX_VALUE);
        assertFalse(policy1.shouldRollover(maxSize, System.currentTimeMillis()));

        long currentMs = System.currentTimeMillis();
        TimeBasedRollingPolicy policy2 = new TimeBasedRollingPolicy(1000);
        assertTrue(policy2.shouldRollover(maxSize, currentMs - 2 * 1000));
    }

    @Test(timeout = 60000)
    public void testSizeBasedRollingPolicy() {
        SizeBasedRollingPolicy policy = new SizeBasedRollingPolicy(1000);
        TestSizable sizable1 = new TestSizable(10);
        assertFalse(policy.shouldRollover(sizable1, 0L));
        TestSizable sizable2 = new TestSizable(10000);
        assertTrue(policy.shouldRollover(sizable2, 0L));
    }
}
