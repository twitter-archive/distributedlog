package com.twitter.distributedlog;

import org.apache.commons.codec.binary.Base64;
import java.nio.ByteBuffer;

public class DLSN implements Comparable<DLSN> {
    static final DLSN InvalidDLSN = new DLSN(0,-1,-1);
    static final byte VERSION = (byte) 0;
    static final int VERSION_LEN = Long.SIZE * 3 + Byte.SIZE;

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
    public int compareTo(DLSN that) {
        if (this.ledgerSequenceNo != that.ledgerSequenceNo) {
            return (this.ledgerSequenceNo < that.ledgerSequenceNo)? -1 : 1;
        } else if (this.entryId != that.entryId) {
            return (this.entryId < that.entryId)? -1 : 1;
        } else {
            return (this.slotId < that.slotId)? -1 : (this.slotId == that.slotId)? 0 : 1;
        }
    }

    public String serialize() {
        byte[] data = new byte[VERSION_LEN];
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.put(VERSION);
        bb.putLong(ledgerSequenceNo);
        bb.putLong(entryId);
        bb.putLong(slotId);
        return Base64.encodeBase64String(data);
    }

    public static DLSN deserialize(String dlsn) {
        byte[] data = Base64.decodeBase64(dlsn);
        ByteBuffer bb = ByteBuffer.wrap(data);
        byte version = bb.get();
        if (VERSION != version || VERSION_LEN != data.length) {
            throw new IllegalArgumentException("Invalid DLSN " + dlsn);
        }
        return new DLSN(bb.getLong(), bb.getLong(), bb.getLong());
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

    /**
     * Positions to a DLSN greater than the current value - this may not
     * correspond to an actual LogRecord, its just used by the positioning logic
     * to position the reader
     *
     * @return the next DLSN
     */
    DLSN getNextDLSN() {
        return new DLSN(ledgerSequenceNo, entryId, slotId + 1);
    }

    /**
     * Positions to a DLSN greater than the current value - this may not
     * correspond to an actual LogRecord, its just used by the positioning logic
     * to position the reader
     *
     * @return the next DLSN
     */
    DLSN positionOnTheNextLedger() {
        return new DLSN(ledgerSequenceNo + 1 , 0, 0);
    }
}
