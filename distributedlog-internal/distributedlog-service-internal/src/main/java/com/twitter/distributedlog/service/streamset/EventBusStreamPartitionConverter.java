package com.twitter.distributedlog.service.streamset;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stream Partition Converter
 */
public class EventBusStreamPartitionConverter extends CacheableStreamPartitionConverter {

    private final Pattern partitionPattern;

    public EventBusStreamPartitionConverter() {
        this.partitionPattern = Pattern.compile("^(?:__dark_)?eventbus_(.+)_(\\d{6})$");
    }

    @Override
    protected Partition newPartition(String streamName) {
        Matcher matcher = partitionPattern.matcher(streamName);
        if (matcher.matches() && matcher.groupCount() == 2) {
            return new Partition(matcher.group(1), Integer.parseInt(matcher.group(2)));
        }
        return new Partition(streamName, 0);
    }
}
