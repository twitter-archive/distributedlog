package com.twitter.distributedlog.injector;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Injector that injects random delays
 */
public class RandomDelayFailureInjector implements FailureInjector {

    private static final Logger LOG = LoggerFactory.getLogger(RandomDelayFailureInjector.class);

    private final DynamicDistributedLogConfiguration dynConf;

    public RandomDelayFailureInjector(DynamicDistributedLogConfiguration dynConf) {
        this.dynConf = dynConf;
    }

    private int delayMs() {
        return dynConf.getEIInjectedWriteDelayMs();
    }

    private double delayPct() {
        return dynConf.getEIInjectedWriteDelayPercent();
    }

    private boolean enabled() {
        return delayMs() > 0 && delayPct() > 0;
    }

    @Override
    public void inject() {
        try {
            if (enabled() && Utils.randomPercent(delayPct())) {
                Thread.sleep(delayMs());
            }
        } catch (InterruptedException ex) {
            LOG.warn("delay was interrupted ", ex);
        }
    }
}
