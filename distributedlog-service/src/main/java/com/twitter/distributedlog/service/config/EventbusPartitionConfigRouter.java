package com.twitter.distributedlog.service.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Map eventbus partition streams to config name using the eventbus partition
 * naming scheme. If the router is unable to match an eventbus partition, it
 * returns the full stream name (like identity matcher).
 */
class EventbusPartitionConfigRouter implements StreamConfigRouter {
    private final Pattern partitionPattern;

    EventbusPartitionConfigRouter() {
        this.partitionPattern = Pattern.compile("^(?:__dark_)?eventbus_(.+)_\\d{6}$");
    }

    public String getConfig(String streamName) {
        Matcher matcher = partitionPattern.matcher(streamName);
        if (matcher.matches() && matcher.groupCount() == 1) {
            return matcher.group(1);
        }
        return streamName;
    }
}
