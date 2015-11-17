package com.twitter.distributedlog.v2;

import java.io.Serializable;
import java.util.Comparator;

public class LedgerReadPosition {

    private static enum PartialOrderingComparisonResult {
        NotComparable,
        GreaterThan,
        LessThan,
        EqualTo
    }


    long ledgerId;
    long ledgerSequenceNo;
    long entryId;

    public LedgerReadPosition(long ledgerId, long ledgerSequenceNo, long entryId) {
        this.ledgerId = ledgerId;
        this.ledgerSequenceNo = ledgerSequenceNo;
        this.entryId = entryId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public void advance() {
        entryId++;
    }

    public void positionOnNewLedger(long ledgerId, long ledgerSequenceNo) {
        this.ledgerId = ledgerId;
        this.ledgerSequenceNo = ledgerSequenceNo;
        this.entryId = 0;
    }

    @Override
    public String toString() {
        return String.format("(lid=%d, lseqNo=%d, eid=%d)", ledgerId, ledgerSequenceNo, entryId);
    }

    public boolean definitelyLessThanOrEqualTo(LedgerReadPosition threshold) {
        PartialOrderingComparisonResult result = comparePartiallyOrdered(threshold);
        return ((result == PartialOrderingComparisonResult.LessThan) ||
                (result == PartialOrderingComparisonResult.EqualTo));
    }

    public boolean definitelyLessThan(LedgerReadPosition threshold) {
        PartialOrderingComparisonResult result = comparePartiallyOrdered(threshold);
        return result == PartialOrderingComparisonResult.LessThan;
    }

    private PartialOrderingComparisonResult comparePartiallyOrdered(LedgerReadPosition threshold) {
        // If no threshold is passed we cannot make a definitive comparison
        if (null == threshold) {
            return PartialOrderingComparisonResult.NotComparable;
        }

        if (this.ledgerSequenceNo != threshold.ledgerSequenceNo) {
            if (this.ledgerSequenceNo < threshold.ledgerSequenceNo) {
                return PartialOrderingComparisonResult.LessThan;
            } else {
                return PartialOrderingComparisonResult.GreaterThan;
            }
        } else if (this.getLedgerId() != threshold.getLedgerId()) {
            // When ledgerSequenceNo is equal we cannot definitely say that this
            // position is less than the threshold unless ledgerIds are equal
            // since LedgerSequenceNumber maybe inferred from transactionIds in older
            // versions of the metadata.
            return PartialOrderingComparisonResult.NotComparable;
        } else if (this.getEntryId() < threshold.getEntryId()) {
            return PartialOrderingComparisonResult.LessThan;
        } else if (this.getEntryId() > threshold.getEntryId()) {
            return PartialOrderingComparisonResult.GreaterThan;
        } else {
            return PartialOrderingComparisonResult.EqualTo;
        }
    }


    /**
     * Comparator for the key portion
     */
    public static final ReadAheadCacheKeyComparator COMPARATOR = new ReadAheadCacheKeyComparator();

    // Only compares the key portion
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LedgerReadPosition)) {
            return false;
        }
        LedgerReadPosition key = (LedgerReadPosition) other;
        return ledgerId == key.ledgerId &&
            entryId == key.entryId;
    }

    @Override
    public int hashCode() {
        return (int) (ledgerId * 13 ^ entryId * 17);
    }

    /**
     * Compare EntryKey.
     */
    protected static class ReadAheadCacheKeyComparator implements Comparator<LedgerReadPosition>, Serializable {

        private static final long serialVersionUID = 0L;

        @Override
        public int compare(LedgerReadPosition left, LedgerReadPosition right) {
            long ret = left.ledgerId - right.ledgerId;
            if (ret == 0) {
                ret = left.entryId - right.entryId;
            }
            return (ret < 0) ? -1 : ((ret > 0) ? 1 : 0);
        }
    }

}


