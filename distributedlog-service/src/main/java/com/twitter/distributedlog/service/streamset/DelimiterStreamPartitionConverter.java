package com.twitter.distributedlog.service.streamset;

import org.apache.commons.lang3.StringUtils;

/**
 * Stream Partition Converter
 */
public class DelimiterStreamPartitionConverter extends CacheableStreamPartitionConverter {

    private final String delimiter;

    public DelimiterStreamPartitionConverter() {
        this("_");
    }

    public DelimiterStreamPartitionConverter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    protected Partition newPartition(String streamName) {
        String[] parts = StringUtils.split(streamName, delimiter);
        if (null != parts && parts.length == 2) {
            try {
                int partition = Integer.parseInt(parts[1]);
                return new Partition(parts[0], partition);
            } catch (NumberFormatException nfe) {
                // ignore the exception
            }
        }
        return new Partition(streamName, 0);
    }
}
