package com.twitter.distributedlog.bk;

/**
 * Provider to provide quorum config
 */
public interface QuorumConfigProvider {

    /**
     * Get the quorum config for a given log stream.
     *
     * @return quorum config
     */
    QuorumConfig getQuorumConfig();

}
