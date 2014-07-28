package com.twitter.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import java.nio.ByteBuffer;

public class DLSN implements Comparable<DLSN> {

    public static final byte VERSION0 = (byte) 0;
    public static final byte VERSION1 = (byte) 1;

    public static final DLSN InitialDLSN = new DLSN(1, 0 , 0);
    static final DLSN InvalidDLSN = new DLSN(0,-1,-1);
    static final byte CUR_VERSION = VERSION1;

    static final int VERSION0_LEN = Long.SIZE * 3 + Byte.SIZE;
    static final int VERSION1_LEN = Long.SIZE * 3 / Byte.SIZE + 1;

    private final long ledgerSequenceNo;
    private final long entryId;
    private final long slotId;

    public DLSN(long ledgerSequenceNo, long entryId, long slotId) {
        this.ledgerSequenceNo = ledgerSequenceNo;
        this.entryId = entryId;
        this.slotId = slotId;
    }

    long getLedgerSequenceNo() {
        return ledgerSequenceNo;
    }

    long getEntryId() {
        return entryId;
    }

    long getSlotId() {
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
        return serialize(CUR_VERSION);
    }

    public String serialize(byte version) {
        Preconditions.checkArgument(version <= CUR_VERSION);
        byte[] data = new byte[CUR_VERSION == version ? VERSION1_LEN : VERSION0_LEN];
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.put(version);
        bb.putLong(ledgerSequenceNo);
        bb.putLong(entryId);
        bb.putLong(slotId);
        return Base64.encodeBase64String(data);
    }

    public static DLSN deserialize(String dlsn) {
        byte[] data = Base64.decodeBase64(dlsn);
        ByteBuffer bb = ByteBuffer.wrap(data);
        byte version = bb.get();
        if (VERSION0 == version) {
            if (VERSION0_LEN != data.length) {
                throw new IllegalArgumentException("Invalid version zero DLSN " + dlsn);
            }
        } else if (VERSION1 == version) {
            if (VERSION1_LEN != data.length) {
                throw new IllegalArgumentException("Invalid version one DLSN " + dlsn);
            }
        } else {
            throw new IllegalArgumentException("Invalid DLSN " + dlsn);
        }
        return new DLSN(bb.getLong(), bb.getLong(), bb.getLong());
    }

    // Keep original version0 logic for testing.
    @VisibleForTesting
    static DLSN deserialize0(String dlsn) {
        byte[] data = Base64.decodeBase64(dlsn);
        ByteBuffer bb = ByteBuffer.wrap(data);
        byte version = bb.get();
        if (VERSION0 != version || VERSION0_LEN != data.length) {
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
    public DLSN getNextDLSN() {
        return new DLSN(ledgerSequenceNo, entryId, slotId + 1);
    }

    /**
     * Positions to a DLSN greater than the current value - this may not
     * correspond to an actual LogRecord, its just used by the positioning logic
     * to position the reader
     *
     * @return the next DLSN
     */
    public DLSN positionOnTheNextLedger() {
        return new DLSN(ledgerSequenceNo + 1 , 0, 0);
    }
}
