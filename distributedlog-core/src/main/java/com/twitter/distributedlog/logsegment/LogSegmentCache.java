package com.twitter.distributedlog.logsegment;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LogSegmentMetadata;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Managing log segments in local cache.
 *
 * <p>
 * Caching of log segment metadata assumes that the data contained in the ZNodes for individual
 * log segments is never updated after creation i.e we never call setData. A log segment
 * is finalized by creating a new ZNode and deleting the in progress node. This code will have
 * to change if we change the behavior
 * </p>
 */
public class LogSegmentCache {

    static final Logger LOG = LoggerFactory.getLogger(LogSegmentCache.class);

    protected final Map<String, LogSegmentMetadata> logSegments =
            new HashMap<String, LogSegmentMetadata>();
    protected final ConcurrentMap<Long, LogSegmentMetadata> lid2LogSegments =
            new ConcurrentHashMap<Long, LogSegmentMetadata>();

    /**
     * Retrieve log segments from the cache.
     *
     * @param comparator
     *          comparator to sort the returned log segments.
     * @param segmentFilter
     *          filter to filter out the returned log segments.
     * @return list of sorted and filtered log segments.
     */
    public List<LogSegmentMetadata> getLogSegments(Comparator<LogSegmentMetadata> comparator,
                                                   LogSegmentFilter segmentFilter) {
        List<LogSegmentMetadata> segmentsToReturn;
        synchronized (logSegments) {
            segmentsToReturn = new ArrayList<LogSegmentMetadata>(logSegments.size());
            Collection<String> segmentNamesFiltered = segmentFilter.filter(logSegments.keySet());
            for (String name : segmentNamesFiltered) {
                segmentsToReturn.add(logSegments.get(name));
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Cached log segments : {}", segmentsToReturn);
            }
        }
        Collections.sort(segmentsToReturn, LogSegmentMetadata.COMPARATOR);
        long startSequenceId = DistributedLogConstants.UNASSIGNED_SEQUENCE_ID;
        LogSegmentMetadata prevMetadata = null;
        for (int i = 0; i < segmentsToReturn.size(); i++) {
            LogSegmentMetadata metadata = segmentsToReturn.get(i);
            if (!metadata.isInProgress()) {
                if (metadata.supportsSequenceId()) {
                    startSequenceId = metadata.getStartSequenceId() + metadata.getRecordCount();
                    if (null != prevMetadata && prevMetadata.supportsSequenceId()
                            && prevMetadata.getStartSequenceId() > metadata.getStartSequenceId()) {
                        LOG.warn("Found decreasing start sequence id in log segment {}, previous is {}",
                                metadata, prevMetadata);
                    }
                } else {
                    startSequenceId = DistributedLogConstants.UNASSIGNED_SEQUENCE_ID;
                }
            } else {
                if (metadata.supportsSequenceId()) {
                    LogSegmentMetadata newMetadata = metadata.mutator()
                            .setStartSequenceId(startSequenceId == DistributedLogConstants.UNASSIGNED_SEQUENCE_ID ? 0L : startSequenceId)
                            .build();
                    segmentsToReturn.set(i, newMetadata);
                }
                break;
            }
            prevMetadata = metadata;
        }
        if (comparator != LogSegmentMetadata.COMPARATOR) {
            Collections.sort(segmentsToReturn, comparator);
        }
        return segmentsToReturn;
    }

    /**
     * Add the segment <i>metadata</i> for <i>name</i> in the cache.
     *
     * @param name
     *          segment name.
     * @param metadata
     *          segment metadata.
     */
    public void add(String name, LogSegmentMetadata metadata) {
        synchronized (logSegments) {
            if (!logSegments.containsKey(name)) {
                logSegments.put(name, metadata);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Added log segment ({} : {}) to cache.", name, metadata);
                }
            }
            LogSegmentMetadata oldMetadata = lid2LogSegments.remove(metadata.getLedgerId());
            if (null == oldMetadata) {
                lid2LogSegments.put(metadata.getLedgerId(), metadata);
            } else {
                if (oldMetadata.isInProgress() && !metadata.isInProgress()) {
                    lid2LogSegments.put(metadata.getLedgerId(), metadata);
                } else {
                    lid2LogSegments.put(oldMetadata.getLedgerId(), oldMetadata);
                }
            }
        }
    }

    /**
     * Retrieve log segment <code>name</code> from the cache.
     *
     * @param name
     *          name of the log segment.
     * @return log segment metadata
     */
    public LogSegmentMetadata get(String name) {
        synchronized (logSegments) {
            return logSegments.get(name);
        }
    }

    /**
     * Diff with new received segment list <code>segmentReceived</code>.
     * (TODO: the logic should be changed)
     *
     * @param segmentsReceived
     *          new received segment list
     * @return segments added (left) and removed (right).
     */
    public Pair<Set<String>, Set<String>> diff(Set<String> segmentsReceived) {
        Set<String> segmentsAdded;
        Set<String> segmentsRemoved;
        synchronized (logSegments) {
            Set<String> segmentsCached = logSegments.keySet();
            segmentsAdded = Sets.difference(segmentsReceived, segmentsCached).immutableCopy();
            segmentsRemoved = Sets.difference(segmentsCached, segmentsReceived).immutableCopy();
            for (String s : segmentsRemoved) {
                LogSegmentMetadata segmentMetadata = remove(s);
                LOG.debug("Removed log segment {} from cache : {}.", s, segmentMetadata);
            }
        }
        return Pair.of(segmentsAdded, segmentsRemoved);
    }

    /**
     * Remove log segment <code>name</code> from the cache.
     *
     * @param name
     *          name of the log segment.
     * @return log segment metadata.
     */
    public LogSegmentMetadata remove(String name) {
        synchronized (logSegments) {
            LogSegmentMetadata metadata = logSegments.remove(name);
            if (null != metadata) {
                lid2LogSegments.remove(metadata.getLedgerId(), metadata);
                LOG.debug("Removed log segment ({} : {}) from cache.", name, metadata);
            }
            return metadata;
        }
    }


}
