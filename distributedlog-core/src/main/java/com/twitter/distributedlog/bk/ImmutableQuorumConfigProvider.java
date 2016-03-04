package com.twitter.distributedlog.bk;

/**
 * Provider that returns an immutable quorum config.
 */
public class ImmutableQuorumConfigProvider implements QuorumConfigProvider {

    private final QuorumConfig quorumConfig;

    public ImmutableQuorumConfigProvider(QuorumConfig quorumConfig) {
        this.quorumConfig = quorumConfig;
    }

    @Override
    public QuorumConfig getQuorumConfig() {
        return quorumConfig;
    }
}
