package com.twitter.distributedlog;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TestLogSegmentFilter {

    private static String completedLedgerZNodeNameWithVersion(long ledgerId, long firstTxId, long lastTxId, long ledgerSeqNo) {
        return String.format("%s_%018d_%018d_%018d_v%dl%d_%04d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                             firstTxId, lastTxId, ledgerSeqNo, DistributedLogConstants.LOGSEGMENT_NAME_VERSION, ledgerId, 1);
    }

    private static String completedLedgerZNodeNameWithTxID(long firstTxId, long lastTxId) {
        return String.format("%s_%018d_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, firstTxId, lastTxId);
    }

    private static String completedLedgerZNodeNameWithLedgerSequenceNumber(long ledgerSeqNo) {
        return String.format("%s_%018d", DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX, ledgerSeqNo);
    }

    @Test
    public void testWriteFilter() {
        Set<String> expectedFilteredSegments = new HashSet<String>();
        List<String> segments = new ArrayList<String>();
        for (int i = 1; i <= 5; i++) {
            segments.add(completedLedgerZNodeNameWithVersion(i, (i - 1) * 100, i * 100 - 1, i));
        }
        for (int i = 6; i <= 10; i++) {
            String segmentName = completedLedgerZNodeNameWithLedgerSequenceNumber(i);
            segments.add(segmentName);
            expectedFilteredSegments.add(segmentName);
        }
        for (int i = 11; i <= 15; i++) {
            String segmentName = completedLedgerZNodeNameWithTxID((i - 1) * 100, i * 100 - 1);
            segments.add(segmentName);
            expectedFilteredSegments.add(segmentName);
        }
        segments.add("");
        segments.add("unknown");
        segments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1234_5678_9");
        expectedFilteredSegments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1234_5678_9");
        segments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1_2_3_4_5_6_7_8_9");
        expectedFilteredSegments.add(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX + "_1_2_3_4_5_6_7_8_9");

        Collection<String> filteredCollection = BKLogPartitionWriteHandler.WRITE_HANDLE_FILTER.filter(segments);
        assertEquals(expectedFilteredSegments.size(), filteredCollection.size());

        Set<String> filteredSegments = Sets.newHashSet(filteredCollection);
        Sets.SetView<String> diff = Sets.difference(filteredSegments, expectedFilteredSegments);
        assertTrue(diff.isEmpty());
    }
}
