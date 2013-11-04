package com.twitter.distributedlog;

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;

public class DLSN {
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
    public String toString() {
        return "DLSN{" +
            "ledgerSequenceNo=" + ledgerSequenceNo +
            ", entryId=" + entryId +
            ", slotId=" + slotId +
            '}';
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
