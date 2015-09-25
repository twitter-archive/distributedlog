package com.twitter.distributedlog.util;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.LogSegmentMetadata;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Test Case for {@link DLUtils}
 */
public class TestDLUtils {

    private static LogSegmentMetadata completedLogSegment(
            long logSegmentSequenceNumber, long fromTxnId, long toTxnId) {
        return DLMTestUtil.completedLogSegment(
                "/logsegment/" + fromTxnId,
                fromTxnId,
                fromTxnId,
                toTxnId,
                100,
                logSegmentSequenceNumber,
                999L,
                0L);
    }

    private static LogSegmentMetadata inprogressLogSegment(
            long logSegmentSequenceNumber, long firstTxId) {
        return DLMTestUtil.inprogressLogSegment(
                "/logsegment/" + firstTxId,
                firstTxId,
                firstTxId,
                logSegmentSequenceNumber);
    }

    @Test(timeout = 60000)
    public void testFindLogSegmentNotLessThanTxnId() throws Exception {
        long txnId = 999L;
        // empty list
        List<LogSegmentMetadata> emptyList = Lists.newArrayList();
        assertEquals(-1, DLUtils.findLogSegmentNotLessThanTxnId(emptyList, txnId));

        // list that all segment's txn id is larger than txn-id-to-search
        List<LogSegmentMetadata> list1 = Lists.newArrayList(
                completedLogSegment(1L, 1000L, 2000L));
        assertEquals(-1, DLUtils.findLogSegmentNotLessThanTxnId(list1, txnId));

        List<LogSegmentMetadata> list2 = Lists.newArrayList(
                inprogressLogSegment(1L, 1000L));
        assertEquals(-1, DLUtils.findLogSegmentNotLessThanTxnId(list2, txnId));

        // the first log segment whose first txn id is less than txn-id-to-search
        List<LogSegmentMetadata> list3 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 99L),
                completedLogSegment(2L, 1000L, 2000L)
        );
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list3, txnId));


        List<LogSegmentMetadata> list4 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 990L),
                completedLogSegment(2L, 1000L, 2000L)
        );
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list4, txnId));

        List<LogSegmentMetadata> list5 = Lists.newArrayList(
                inprogressLogSegment(1L, 0L),
                inprogressLogSegment(2L, 1000L)
        );
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list5, txnId));

        // list that all segment's txn id is less than txn-id-to-search
        List<LogSegmentMetadata> list6 = Lists.newArrayList(
                completedLogSegment(1L, 100L, 200L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list6, txnId));

        List<LogSegmentMetadata> list7 = Lists.newArrayList(
                inprogressLogSegment(1L, 100L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list7, txnId));

        // list that first segment's first txn id equals to txn-id-to-search
        List<LogSegmentMetadata> list8 = Lists.newArrayList(
                completedLogSegment(1L, 999L, 2000L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list8, txnId));

        List<LogSegmentMetadata> list9 = Lists.newArrayList(
                inprogressLogSegment(1L, 999L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list9, txnId));

        List<LogSegmentMetadata> list10 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 999L),
                completedLogSegment(2L, 999L, 2000L));
        assertEquals(0, DLUtils.findLogSegmentNotLessThanTxnId(list10, txnId));

        List<LogSegmentMetadata> list11 = Lists.newArrayList(
                completedLogSegment(1L, 0L, 99L),
                completedLogSegment(2L, 999L, 2000L));
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list11, txnId));

        List<LogSegmentMetadata> list12 = Lists.newArrayList(
                inprogressLogSegment(1L, 0L),
                inprogressLogSegment(2L, 999L));
        assertEquals(1, DLUtils.findLogSegmentNotLessThanTxnId(list12, txnId));
    }

}
