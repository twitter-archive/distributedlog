package com.twitter.distributedlog.client.speculative;

import com.twitter.util.Future;

public interface SpeculativeRequestExecutor {

    /**
     * Issues a speculative request and indicates if more speculative
     * requests should be issued
     *
     * @return whether more speculative requests should be issued
     */
    Future<Boolean> issueSpeculativeRequest();
}
