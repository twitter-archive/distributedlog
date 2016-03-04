package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;

/**
 * Provider returns quorum configs based on dynamic configuration.
 */
public class DynamicQuorumConfigProvider implements QuorumConfigProvider {

    private final DynamicDistributedLogConfiguration conf;

    public DynamicQuorumConfigProvider(DynamicDistributedLogConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public QuorumConfig getQuorumConfig() {
        return conf.getQuorumConfig();
    }
}
