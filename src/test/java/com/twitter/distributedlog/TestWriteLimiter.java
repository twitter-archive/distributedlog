package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import scala.runtime.BoxedUnit;

public class TestWriteLimiter {
    static final Logger LOG = LoggerFactory.getLogger(TestWriteLimiter.class);

    SimplePermitLimiter createPermitLimiter(boolean darkmode, int permits) {
        return new SimplePermitLimiter(darkmode, permits, new NullStatsLogger(), false);
    }

    @Test
    public void testGlobalOnly() throws Exception {
        SimplePermitLimiter streamLimiter = createPermitLimiter(false, Integer.MAX_VALUE);
        SimplePermitLimiter globalLimiter = createPermitLimiter(false, 1);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        try {
            limiter.acquire();
            fail("should have thrown global limit exception");
        } catch (OverCapacityException ex) {
        }
        assertPermits(streamLimiter, 1, globalLimiter, 1);
        limiter.release();
        assertPermits(streamLimiter, 0, globalLimiter, 0);
    }

    @Test
    public void testStreamOnly() throws Exception {
        SimplePermitLimiter streamLimiter = createPermitLimiter(false, 1);
        SimplePermitLimiter globalLimiter = createPermitLimiter(false, Integer.MAX_VALUE);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        try {
            limiter.acquire();
            fail("should have thrown stream limit exception");
        } catch (OverCapacityException ex) {
        }
        assertPermits(streamLimiter, 1, globalLimiter, 1);
    }

    @Test
    public void testDarkmode() throws Exception {
        SimplePermitLimiter streamLimiter = createPermitLimiter(true, Integer.MAX_VALUE);
        SimplePermitLimiter globalLimiter = createPermitLimiter(true, 1);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        limiter.acquire();
        assertPermits(streamLimiter, 2, globalLimiter, 2);
    }

    @Test
    public void testDarkmodeGlobalUnderStreamOver() throws Exception {
        SimplePermitLimiter streamLimiter = createPermitLimiter(true, 1);
        SimplePermitLimiter globalLimiter = createPermitLimiter(true, 2);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        limiter.acquire();
        assertPermits(streamLimiter, 2, globalLimiter, 2);
        limiter.release();
    }

    @Test
    public void testDarkmodeGlobalOverStreamUnder() throws Exception {
        SimplePermitLimiter streamLimiter = createPermitLimiter(true, 2);
        SimplePermitLimiter globalLimiter = createPermitLimiter(true, 1);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        limiter.acquire();
        assertPermits(streamLimiter, 2, globalLimiter, 2);
        limiter.release();
        assertPermits(streamLimiter, 1, globalLimiter, 1);
        limiter.release();
        assertPermits(streamLimiter, 0, globalLimiter, 0);
    }

    void assertPermits(SimplePermitLimiter streamLimiter, int streamPermits, SimplePermitLimiter globalLimiter, int globalPermits) {
        assertEquals(streamPermits, streamLimiter.getPermits());
        assertEquals(globalPermits, globalLimiter.getPermits());
    }
}
