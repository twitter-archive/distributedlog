package com.twitter.distributedlog.impl;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DistributedLogConstants;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.twitter.distributedlog.impl.ZKLogSegmentFilters.*;
import static org.junit.Assert.*;

public class TestZKLogSegmentFilters {

    static final Logger LOG = LoggerFactory.getLogger(TestZKLogSegmentFilters.class);

    @Test
    public void testWriteFilter() {
        Set<String> expectedFilteredSegments = new HashSet<String>();
        List<String> segments = new ArrayList<String>();
        for (int i = 1; i <= 5; i++) {
            segments.add(DLMTestUtil.completedLedgerZNodeNameWithVersion(i, (i - 1) * 100, i * 100 - 1, i));
        }
        for (int i = 6; i <= 10; i++) {
            String segmentName = DLMTestUtil.completedLedgerZNodeNameWithLogSegmentSequenceNumber(i);
            segments.add(segmentName);
            if (i == 10) {
                expectedFilteredSegments.add(segmentName);
            }
        }
        for (int i = 11; i <= 15; i++) {
            String segmentName = DLMTestUtil.completedLedgerZNodeNameWithTxID((i - 1) * 100, i * 100 - 1);
            segments.add(segmentName);
            expectedFilteredSegments.add(segmentName);
        }
        segments.add("");
        segments.add("unknown");
        segments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1234_5678_9");
        expectedFilteredSegments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1234_5678_9");
        segments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1_2_3_4_5_6_7_8_9");
        expectedFilteredSegments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1_2_3_4_5_6_7_8_9");

        Collection<String> filteredCollection = WRITE_HANDLE_FILTER.filter(segments);
        LOG.info("Filter log segments {} to {}.", segments, filteredCollection);
        assertEquals(expectedFilteredSegments.size(), filteredCollection.size());

        Set<String> filteredSegments = Sets.newHashSet(filteredCollection);
        Sets.SetView<String> diff = Sets.difference(filteredSegments, expectedFilteredSegments);
        assertTrue(diff.isEmpty());
    }
}
