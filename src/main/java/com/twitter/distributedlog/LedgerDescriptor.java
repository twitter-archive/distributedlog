package com.twitter.distributedlog;

public class LedgerDescriptor {
    private final long ledgerId;
    private final boolean fenced;

    public LedgerDescriptor(LedgerDescriptor that) {
        this.ledgerId = that.ledgerId;
        this.fenced = that.fenced;
    }

    public LedgerDescriptor(long ledgerId, boolean fenced) {
        this.ledgerId = ledgerId;
        this.fenced = fenced;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public boolean getFenced() {
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
}