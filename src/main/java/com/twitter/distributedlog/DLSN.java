package com.twitter.distributedlog;

public class DLSN {
    private final long ledgerSequenceNo;
    private final long entryId;
    private final long slotId;

    public DLSN(long ledgerSequenceNo, long entryId, long slotId) {
        this.ledgerSequenceNo = ledgerSequenceNo;
        this.entryId = entryId;
        this.slotId = slotId;
    }

    public long getLedgerSequenceNo() {
        return ledgerSequenceNo;
    }

    public long getEntryId() {
        return entryId;
    }

    public long getSlotId() {
        return slotId;
    }

    @Override
    public String toString() {
        return "DLSN{" +
            "ledgerSequenceNo=" + ledgerSequenceNo +
            ", entryId=" + entryId +
            ", slotId=" + slotId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DLSN)) return false;

        DLSN dlsn = (DLSN) o;

        if (entryId != dlsn.entryId) return false;
        if (ledgerSequenceNo != dlsn.ledgerSequenceNo) return false;
        if (slotId != dlsn.slotId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (ledgerSequenceNo ^ (ledgerSequenceNo >>> 32));
        result = 31 * result + (int) (entryId ^ (entryId >>> 32));
        result = 31 * result + (int) (slotId ^ (slotId >>> 32));
        return result;
    }
}
