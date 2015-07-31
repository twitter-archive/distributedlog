package com.twitter.distributedlog.logsegment;

import java.util.Collection;

/**
 * Filter to filter log segments
 */
public interface LogSegmentFilter {

    public static final LogSegmentFilter DEFAULT_FILTER = new LogSegmentFilter() {
        @Override
        public Collection<String> filter(Collection<String> fullList) {
            return fullList;
        }
    };

    /**
     * Filter the log segments from the full log segment list.
     *
     * @param fullList
     *          full list of log segment names.
     * @return filtered log segment names
     */
    Collection<String> filter(Collection<String> fullList);
}
