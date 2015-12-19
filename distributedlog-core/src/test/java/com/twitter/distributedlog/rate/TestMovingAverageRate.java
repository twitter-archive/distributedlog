package com.twitter.distributedlog.rate;

import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.MockTimer;
import com.twitter.util.Time$;
import com.twitter.util.TimeControl;

import org.junit.Test;
import scala.runtime.BoxedUnit;

import static org.junit.Assert.*;

public class TestMovingAverageRate {
    interface TcCallback {
        void apply(TimeControl tc);
    }

    void withCurrentTimeFrozen(final TcCallback cb) {
        Time$.MODULE$.withCurrentTimeFrozen(new Function<TimeControl, BoxedUnit>() {
            @Override
            public BoxedUnit apply(TimeControl time) {
                cb.apply(time);
                return BoxedUnit.UNIT;
            }
        });
    }

    private void advance(TimeControl time, MockTimer timer, int timeMs) {
        Duration duration = Duration.fromMilliseconds(timeMs);
        time.advance(duration);
        timer.tick();
    }

    @Test(timeout = 60000)
    public void testNoChangeInUnderMinInterval() {
        withCurrentTimeFrozen(new TcCallback() {
            @Override
            public void apply(TimeControl time) {
                MockTimer timer = new MockTimer();
                MovingAverageRateFactory factory = new MovingAverageRateFactory(timer);
                MovingAverageRate avg60 = factory.create(60);
                avg60.add(1000);
                assertEquals(0, avg60.get(), 0);
                advance(time, timer, 1);
                assertEquals(0, avg60.get(), 0);
                advance(time, timer, 1);
                assertEquals(0, avg60.get(), 0);
            }
        });
    }

    @Test(timeout = 60000)
    public void testFactoryWithMultipleTimers() {
        withCurrentTimeFrozen(new TcCallback() {
            @Override
            public void apply(TimeControl time) {
                MockTimer timer = new MockTimer();
                MovingAverageRateFactory factory = new MovingAverageRateFactory(timer);
                MovingAverageRate avg60 = factory.create(60);
                MovingAverageRate avg30 = factory.create(30);

                // Can't test this precisely because the Rate class uses its own
                // ticker. So we can control when it gets sampled but not the time
                // value it uses. So, just do basic validation.
                for (int i = 0; i < 30; i++) {
                    avg60.add(100);
                    avg30.add(100);
                    advance(time, timer, 1000);
                }
                double s1 = avg60.get();
                assertTrue(avg30.get() > 0);
                for (int i = 0; i < 30; i++) {
                    advance(time, timer, 1000);
                }
                assertTrue(avg60.get() > 0);
                assertTrue(avg60.get() < s1);
                assertEquals(0.0, avg30.get(), 0);
            }
        });
    }
}
