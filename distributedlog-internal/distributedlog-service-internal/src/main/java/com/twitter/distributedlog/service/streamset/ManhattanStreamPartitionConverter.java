package com.twitter.distributedlog.service.streamset;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class ManhattanStreamPartitionConverter extends CacheableStreamPartitionConverter {

    private final Pattern partitionPattern;

    public ManhattanStreamPartitionConverter() {
        this.partitionPattern = Pattern.compile("^manhattan-(?:xdc-)?rlog-(.+)-(\\d+)$");
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
