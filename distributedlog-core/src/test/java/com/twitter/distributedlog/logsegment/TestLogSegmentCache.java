package com.twitter.distributedlog.logsegment;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test Case for Log Segment Cache.
 */
public class TestLogSegmentCache {

    @Test(timeout = 60000)
    public void testBasicOperations() {
        LogSegmentMetadata metadata =
                DLMTestUtil.completedLogSegment("/segment1", 1L, 1L, 100L, 100, 1L, 99L, 0L);
        String name = DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(1L);

        LogSegmentCache cache = new LogSegmentCache("test-basic-operations");
        assertNull("No log segment " + name + " should be cached", cache.get(name));
        cache.add(name, metadata);
        LogSegmentMetadata metadataRetrieved = cache.get(name);
        assertNotNull("log segment " + name + " should be cached", metadataRetrieved);
        assertEquals("Wrong log segment metadata returned for " + name,
                metadata, metadataRetrieved);
        LogSegmentMetadata metadataRemoved = cache.remove(name);
        assertNull("log segment " + name + " should be removed from cache", cache.get(name));
        assertEquals("Wrong log segment metadata removed for " + name,
                metadata, metadataRemoved);
        assertNull("No log segment " + name + " to be removed", cache.remove(name));
    }

    @Test(timeout = 60000)
    public void testDiff() {
        LogSegmentCache cache = new LogSegmentCache("test-diff");
        // add 5 completed log segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentMetadata metadata =
                    DLMTestUtil.completedLogSegment("/segment" + i, i, i, i * 100L, 100, i, 99L, 0L);
            String name = DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i);
            cache.add(name, metadata);
        }
        // add one inprogress log segment
        LogSegmentMetadata inprogress =
                DLMTestUtil.inprogressLogSegment("/inprogress-6", 6, 600L, 6);
        String name = DLMTestUtil.inprogressZNodeName(6);
        cache.add(name, inprogress);

        // deleted first 2 completed log segments and completed the last one
        Set<String> segmentRemoved = Sets.newHashSet();
        for (int i = 1; i <= 2; i++) {
            segmentRemoved.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i));
        }
        segmentRemoved.add((DLMTestUtil.inprogressZNodeName(6)));
        Set<String> segmentReceived = Sets.newHashSet();
        Set<String> segmentAdded = Sets.newHashSet();
        for (int i = 3; i <= 6; i++) {
            segmentReceived.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i));
            if (i == 6) {
                segmentAdded.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i));
            }
        }

        Pair<Set<String>, Set<String>> segmentChanges = cache.diff(segmentReceived);
        assertTrue("Should remove " + segmentRemoved + ", but removed " + segmentChanges.getRight(),
                Sets.difference(segmentRemoved, segmentChanges.getRight()).isEmpty());
        assertTrue("Should add " + segmentAdded + ", but added " + segmentChanges.getLeft(),
                Sets.difference(segmentAdded, segmentChanges.getLeft()).isEmpty());
    }

    @Test(timeout = 60000)
    public void testUpdate() {
        LogSegmentCache cache = new LogSegmentCache("test-update");
        // add 5 completed log segments
        for (int i = 1; i <= 5; i++) {
            LogSegmentMetadata metadata =
                    DLMTestUtil.completedLogSegment("/segment" + i, i, i, i * 100L, 100, i, 99L, 0L);
            String name = DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i);
            cache.add(name, metadata);
        }
        // add one inprogress log segment
        LogSegmentMetadata inprogress =
                DLMTestUtil.inprogressLogSegment("/inprogress-6", 6, 600L, 6);
        String name = DLMTestUtil.inprogressZNodeName(6);
        cache.add(name, inprogress);

        // deleted first 2 completed log segments and completed the last one
        Set<String> segmentRemoved = Sets.newHashSet();
        for (int i = 1; i <= 2; i++) {
            segmentRemoved.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i));
        }
        segmentRemoved.add((DLMTestUtil.inprogressZNodeName(6)));
        Set<String> segmentReceived = Sets.newHashSet();
        Map<String, LogSegmentMetadata> segmentAdded = Maps.newHashMap();
        for (int i = 3; i <= 6; i++) {
            segmentReceived.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i));
            if (i == 6) {
                segmentAdded.put(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(i),
                        DLMTestUtil.completedLogSegment("/segment" + i, i, i, i * 100L, 100, i, 99L, 0L));
            }
        }

        // update the cache
        cache.update(segmentRemoved, segmentAdded);
        for (String segment : segmentRemoved) {
            assertNull("Segment " + segment + " should be removed.", cache.get(segment));
        }
        for (String segment : segmentReceived) {
            assertNotNull("Segment " + segment + " should not be removed", cache.get(segment));
        }
        for (Map.Entry<String, LogSegmentMetadata> entry : segmentAdded.entrySet()) {
            assertEquals("Segment " + entry.getKey() + " should be added.",
                    entry.getValue(), entry.getValue());
        }
    }

    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testGapDetection() throws Exception {
        LogSegmentCache cache = new LogSegmentCache("test-gap-detection");
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(1L),
                DLMTestUtil.completedLogSegment("/segment-1", 1L, 1L, 100L, 100, 1L, 99L, 0L));
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(3L),
                DLMTestUtil.completedLogSegment("/segment-3", 3L, 3L, 300L, 100, 3L, 99L, 0L));
        cache.getLogSegments(LogSegmentMetadata.COMPARATOR);
    }

    @Test(timeout = 60000)
    public void testSameLogSegment() throws Exception {
        LogSegmentCache cache = new LogSegmentCache("test-same-log-segment");
        List<LogSegmentMetadata> expectedList = Lists.newArrayListWithExpectedSize(2);
        LogSegmentMetadata inprogress =
                DLMTestUtil.inprogressLogSegment("/inprogress-1", 1L, 1L, 1L);
        expectedList.add(inprogress);
        cache.add(DLMTestUtil.inprogressZNodeName(1L), inprogress);
        LogSegmentMetadata completed =
                DLMTestUtil.completedLogSegment("/segment-1", 1L, 1L, 100L, 100, 1L, 99L, 0L);
        expectedList.add(completed);
        cache.add(DLMTestUtil.completedLedgerZNodeNameWithLedgerSequenceNumber(1L), completed);

        List<LogSegmentMetadata> retrievedList = cache.getLogSegments(LogSegmentMetadata.COMPARATOR);
        assertEquals("Should get both log segments in ascending order",
                expectedList.size(), retrievedList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            assertEqualsWithoutSequenceId(expectedList.get(i), retrievedList.get(i));
        }
        assertEquals("inprogress log segment should see start sequence id : 0",
                0L, retrievedList.get(0).getStartSequenceId());
        Collections.reverse(expectedList);
        retrievedList = cache.getLogSegments(LogSegmentMetadata.DESC_COMPARATOR);
        assertEquals("Should get both log segments in descending order",
                expectedList.size(), retrievedList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            assertEqualsWithoutSequenceId(expectedList.get(i), retrievedList.get(i));
        }
        assertEquals("inprogress log segment should see start sequence id : 0",
                0L, retrievedList.get(1).getStartSequenceId());
    }

    private static void assertEqualsWithoutSequenceId(LogSegmentMetadata m1, LogSegmentMetadata m2) {
        assertEquals("expected " + m1 + " but got " + m2,
                m1.getLedgerSequenceNumber(), m2.getLedgerSequenceNumber());
        assertEquals("expected " + m1 + " but got " + m2,
                m1.getLedgerId(), m2.getLedgerId());
        assertEquals("expected " + m1 + " but got " + m2,
                m1.getFirstTxId(), m2.getFirstTxId());
        assertEquals("expected " + m1 + " but got " + m2,
                m1.getLastTxId(), m2.getLastTxId());
        assertEquals("expected " + m1 + " but got " + m2,
                m1.getLastDLSN(), m2.getLastDLSN());
        assertEquals("expected " + m1 + " but got " + m2,
                m1.getRecordCount(), m2.getRecordCount());
        assertEquals("expected " + m1 + " but got " + m2,
                m1.isInProgress(), m2.isInProgress());
    }

}
