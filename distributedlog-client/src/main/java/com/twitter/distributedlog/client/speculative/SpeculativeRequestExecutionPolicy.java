package com.twitter.distributedlog.client.speculative;

import java.util.concurrent.ScheduledExecutorService;

public interface SpeculativeRequestExecutionPolicy {
    /**
     * Initialize the speculative request execution policy and initiate requests
     *
     * @param scheduler The scheduler service to issue the speculative request
     * @param requestExecutor The executor is used to issue the actual speculative requests
     */
    void initiateSpeculativeRequest(ScheduledExecutorService scheduler,
                                    SpeculativeRequestExecutor requestExecutor);
}
