package com.twitter.distributedlog.callback;

import java.util.List;

/**
 * Listener on list of log segments changes for a given stream.
 */
public interface LogSegmentNamesListener {
    /**
     * Notified when <i>segments</i> updated. The new log segments
     * list is returned in this method.
     *
     * @param segments
     *          updated list of segments.
     */
    void onSegmentsUpdated(List<String> segments);
}
