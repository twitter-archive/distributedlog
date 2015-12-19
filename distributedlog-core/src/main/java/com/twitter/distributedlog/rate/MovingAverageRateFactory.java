package com.twitter.distributedlog.rate;

import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.TimerTask;
import com.twitter.util.Timer;
import com.twitter.util.Time;
import java.util.concurrent.CopyOnWriteArrayList;
import scala.runtime.BoxedUnit;

public class MovingAverageRateFactory {
    private final Timer timer;
    private final TimerTask timerTask;
    private final CopyOnWriteArrayList<SampledMovingAverageRate> avgs;
    private final int DEFAULT_INTERVAL_SECS = 1;

    public MovingAverageRateFactory(Timer timer) {
        this.avgs = new CopyOnWriteArrayList<SampledMovingAverageRate>();
        this.timer = timer;
        Function0<BoxedUnit> sampleTask = new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                sampleAll();
                return null;
            }
        };
        this.timerTask = timer.schedulePeriodically(
            Time.now(), Duration.fromSeconds(DEFAULT_INTERVAL_SECS), sampleTask);
    }

    public MovingAverageRate create(int intervalSecs) {
        SampledMovingAverageRate avg = new SampledMovingAverageRate(intervalSecs);
        avgs.add(avg);
        return avg;
    }

    public void close() {
        timerTask.cancel();
        avgs.clear();
    }

    private void sampleAll() {
        for (SampledMovingAverageRate avg : avgs) {
            avg.sample();
        }
    }
}