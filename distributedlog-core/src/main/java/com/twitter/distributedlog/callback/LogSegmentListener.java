package com.twitter.distributedlog.callback;

import com.twitter.distributedlog.LogSegmentMetadata;

import java.util.List;

/**
 * Listener on log segments changes for a given stream.
 */
public interface LogSegmentListener {

    /**
     * Notified when <i>segments</i> updated. The new sorted log segments
     * list is returned in this method.
     *
     * @param segments
     *          updated list of segments.
     */
    void onSegmentsUpdated(List<LogSegmentMetadata> segments);
}
