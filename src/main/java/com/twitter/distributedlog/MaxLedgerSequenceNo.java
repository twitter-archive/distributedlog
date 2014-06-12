package com.twitter.distributedlog;

import com.twitter.distributedlog.zk.DataWithStat;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing and reading max ledger sequence number
 */
class MaxLedgerSequenceNo {

    int zkVersion;
    long maxSeqNo;

    MaxLedgerSequenceNo(DataWithStat dataWithStat) {
        if (dataWithStat.exists()) {
            zkVersion = dataWithStat.getStat().getVersion();
            try {
                maxSeqNo = toLedgerSequenceNo(dataWithStat.getData());
            } catch (NumberFormatException nfe) {
                maxSeqNo = DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO;
            }
        } else {
            maxSeqNo = DistributedLogConstants.UNASSIGNED_LEDGER_SEQNO;
            zkVersion = -1;
        }
    }

    synchronized int getZkVersion() {
        return zkVersion;
    }

    synchronized long getSequenceNumber() {
        return maxSeqNo;
    }

    synchronized MaxLedgerSequenceNo update(int zkVersion, long ledgerSeqNo) {
        this.zkVersion = zkVersion;
        this.maxSeqNo = ledgerSeqNo;
        return this;
    }

    static long toLedgerSequenceNo(byte[] data) {
        String seqNoStr = new String(data, UTF_8);
        return Long.valueOf(seqNoStr);
    }

    static byte[] toBytes(long ledgerSeqNo) {
        return Long.toString(ledgerSeqNo).getBytes(UTF_8);
    }
}
