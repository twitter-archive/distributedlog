package com.twitter.distributedlog;

import java.io.Serializable;
import java.util.Comparator;

public class LedgerReadPosition {
    long ledgerId;
    long entryId;

    public LedgerReadPosition() {
        this(0, 0);
    }

    public LedgerReadPosition(LedgerReadPosition that) {
        this.ledgerId = that.ledgerId;
        this.entryId = that.entryId;
    }

    public LedgerReadPosition(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
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

    public void positionOnNewLedger(long ledgerId) {
        this.ledgerId = ledgerId;
        this.entryId = 0;
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
        LedgerReadPosition key = (LedgerReadPosition)other;
        return ledgerId == key.ledgerId &&
            entryId == key.entryId;
    }

    @Override
    public int hashCode() {
        return (int)(ledgerId * 13 ^ entryId * 17);
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
            return (ret < 0)? -1 : ((ret > 0)? 1 : 0);
        }
    }

}


