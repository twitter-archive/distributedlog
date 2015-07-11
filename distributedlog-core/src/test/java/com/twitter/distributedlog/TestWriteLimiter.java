package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.OverCapacityException;
import com.twitter.distributedlog.util.PermitLimiter;
import com.twitter.distributedlog.util.SimplePermitLimiter;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.SettableFeature;
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
        return createPermitLimiter(darkmode, permits, new SettableFeature("", 0));
    }

    SimplePermitLimiter createPermitLimiter(boolean darkmode, int permits, Feature feature) {
        return new SimplePermitLimiter(darkmode, permits, new NullStatsLogger(), false, feature);
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
    public void testDarkmodeWithDisabledFeature() throws Exception {
        SettableFeature feature = new SettableFeature("test", 10000);
        SimplePermitLimiter streamLimiter = createPermitLimiter(true, 1, feature);
        SimplePermitLimiter globalLimiter = createPermitLimiter(true, Integer.MAX_VALUE, feature);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        limiter.acquire();
        assertPermits(streamLimiter, 2, globalLimiter, 2);
        limiter.release();
        limiter.release();
        assertPermits(streamLimiter, 0, globalLimiter, 0);
    }

    @Test
    public void testDisabledFeature() throws Exception {
        // Disable darkmode, but should still ignore limits because of the feature.
        SettableFeature feature = new SettableFeature("test", 10000);
        SimplePermitLimiter streamLimiter = createPermitLimiter(false, 1, feature);
        SimplePermitLimiter globalLimiter = createPermitLimiter(false, Integer.MAX_VALUE, feature);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        limiter.acquire();
        assertPermits(streamLimiter, 2, globalLimiter, 2);
        limiter.release();
        limiter.release();
        assertPermits(streamLimiter, 0, globalLimiter, 0);
    }

    @Test
    public void testSetDisableFeatureAfterAcquireAndBeforeRelease() throws Exception {
        SettableFeature feature = new SettableFeature("test", 0);
        SimplePermitLimiter streamLimiter = createPermitLimiter(false, 2, feature);
        SimplePermitLimiter globalLimiter = createPermitLimiter(false, Integer.MAX_VALUE, feature);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        limiter.acquire();
        assertPermits(streamLimiter, 2, globalLimiter, 2);
        feature.set(10000);
        limiter.release();
        limiter.release();
        assertPermits(streamLimiter, 0, globalLimiter, 0);
    }

    @Test
    public void testUnsetDisableFeatureAfterPermitsExceeded() throws Exception {
        SettableFeature feature = new SettableFeature("test", 10000);
        SimplePermitLimiter streamLimiter = createPermitLimiter(false, 1, feature);
        SimplePermitLimiter globalLimiter = createPermitLimiter(false, Integer.MAX_VALUE, feature);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        limiter.acquire();
        limiter.acquire();
        limiter.acquire();
        assertPermits(streamLimiter, 4, globalLimiter, 4);
        feature.set(0);
        limiter.release();
        assertPermits(streamLimiter, 3, globalLimiter, 3);
        try {
            limiter.acquire();
            fail("should have thrown stream limit exception");
        } catch (OverCapacityException ex) {
        }
        assertPermits(streamLimiter, 3, globalLimiter, 3);
        limiter.release();
        limiter.release();
        limiter.release();
        assertPermits(streamLimiter, 0, globalLimiter, 0);
    }

    @Test
    public void testUnsetDisableFeatureBeforePermitsExceeded() throws Exception {
        SettableFeature feature = new SettableFeature("test", 0);
        SimplePermitLimiter streamLimiter = createPermitLimiter(false, 1, feature);
        SimplePermitLimiter globalLimiter = createPermitLimiter(false, Integer.MAX_VALUE, feature);
        WriteLimiter limiter = new WriteLimiter("test", streamLimiter, globalLimiter);
        limiter.acquire();
        try {
            limiter.acquire();
            fail("should have thrown stream limit exception");
        } catch (OverCapacityException ex) {
        }
        assertPermits(streamLimiter, 1, globalLimiter, 1);
        feature.set(10000);
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
        limiter.release();
        assertPermits(streamLimiter, 0, globalLimiter, 0);
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
