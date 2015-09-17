package com.twitter.distributedlog;

class LedgerDescriptor {
    private final long ledgerId;
    private final long logSegmentSequenceNo;
    private final boolean fenced;

    public LedgerDescriptor(long ledgerId, long logSegmentSequenceNo, boolean fenced) {
        this.ledgerId = ledgerId;
        this.logSegmentSequenceNo = logSegmentSequenceNo;
        this.fenced = fenced;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getLogSegmentSequenceNo() {
        return logSegmentSequenceNo;
    }

    public boolean isFenced() {
        return fenced;
    }

    // Only compares the key portion
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LedgerDescriptor)) {
            return false;
        }
        LedgerDescriptor key = (LedgerDescriptor) other;
        return ledgerId == key.ledgerId &&
            fenced == key.fenced;
    }

    @Override
    public int hashCode() {
        return (int) (ledgerId * 13 ^ (fenced ? 0xFFFF : 0xF0F0) * 17);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(lid=").append(ledgerId).append(", lseqno=").append(logSegmentSequenceNo)
                .append(", fenced=").append(fenced).append(")");
        return sb.toString();
    }
}

