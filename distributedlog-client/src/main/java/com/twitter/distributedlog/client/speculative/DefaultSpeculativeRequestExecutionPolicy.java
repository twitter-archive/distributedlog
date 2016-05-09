package com.twitter.distributedlog.client.speculative;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSpeculativeRequestExecutionPolicy implements SpeculativeRequestExecutionPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSpeculativeRequestExecutionPolicy.class);
    final int firstSpeculativeRequestTimeout;
    final int maxSpeculativeRequestTimeout;
    final float backoffMultiplier;
    int nextSpeculativeRequestTimeout;

    public DefaultSpeculativeRequestExecutionPolicy(int firstSpeculativeRequestTimeout,
                                                    int maxSpeculativeRequestTimeout,
                                                    float backoffMultiplier) {
        this.firstSpeculativeRequestTimeout = firstSpeculativeRequestTimeout;
        this.maxSpeculativeRequestTimeout = maxSpeculativeRequestTimeout;
        this.backoffMultiplier = backoffMultiplier;
        this.nextSpeculativeRequestTimeout = firstSpeculativeRequestTimeout;

        if (backoffMultiplier <= 0) {
            throw new IllegalArgumentException("Invalid value provided for backoffMultiplier");
        }

        // Prevent potential over flow
        if (Math.round((double)maxSpeculativeRequestTimeout * (double)backoffMultiplier) > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid values for maxSpeculativeRequestTimeout and backoffMultiplier");
        }
    }

    @VisibleForTesting
    int getNextSpeculativeRequestTimeout() {
        return nextSpeculativeRequestTimeout;
    }

    /**
     * Initialize the speculative request execution policy
     *
     * @param scheduler The scheduler service to issue the speculative request
     * @param requestExecutor The executor is used to issue the actual speculative requests
     */
    @Override
    public void initiateSpeculativeRequest(final ScheduledExecutorService scheduler,
                                           final SpeculativeRequestExecutor requestExecutor) {
        issueSpeculativeRequest(scheduler, requestExecutor);
    }

    private void issueSpeculativeRequest(final ScheduledExecutorService scheduler,
                                         final SpeculativeRequestExecutor requestExecutor) {
        Future<Boolean> issueNextRequest = requestExecutor.issueSpeculativeRequest();
        issueNextRequest.addEventListener(new FutureEventListener<Boolean>() {
            // we want this handler to run immediately after we push the big red button!
            @Override
            public void onSuccess(Boolean issueNextRequest) {
                if (issueNextRequest) {
                    scheduleSpeculativeRequest(scheduler, requestExecutor, nextSpeculativeRequestTimeout);
                    nextSpeculativeRequestTimeout = Math.min(maxSpeculativeRequestTimeout,
                            (int) (nextSpeculativeRequestTimeout * backoffMultiplier));
                } else {
                    if(LOG.isTraceEnabled()) {
                        LOG.trace("Stopped issuing speculative requests for {}, " +
                                "speculativeReadTimeout = {}", requestExecutor, nextSpeculativeRequestTimeout);
                    }
                }
            }

            @Override
            public void onFailure(Throwable thrown) {
                LOG.warn("Failed to issue speculative request for {}, speculativeReadTimeout = {} : ",
                        new Object[] { requestExecutor, nextSpeculativeRequestTimeout, thrown });
            }
        });
    }

    private void scheduleSpeculativeRequest(final ScheduledExecutorService scheduler,
                                            final SpeculativeRequestExecutor requestExecutor,
                                            final int speculativeRequestTimeout) {
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    issueSpeculativeRequest(scheduler, requestExecutor);
                }
            }, speculativeRequestTimeout, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException re) {
            if (!scheduler.isShutdown()) {
                LOG.warn("Failed to schedule speculative request for {}, speculativeReadTimeout = {} : ",
                        new Object[]{requestExecutor, speculativeRequestTimeout, re});
            }
        }
    }
}
