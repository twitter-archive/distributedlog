package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.zk.DataWithStat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing and reading max ledger sequence number
 */
class MaxLedgerSequenceNo {

    static final Logger LOG = LoggerFactory.getLogger(MaxLedgerSequenceNo.class);

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

    synchronized void store(ZooKeeperClient zkc, String path, long ledgerSeqNo) throws IOException {
        try {
            Stat stat = zkc.get().setData(path, toBytes(ledgerSeqNo), zkVersion);
            this.zkVersion = stat.getVersion();
            this.maxSeqNo = ledgerSeqNo;
        } catch (KeeperException ke) {
            throw new ZKException("Error writing max ledger sequence number " + ledgerSeqNo + " to "
                                  + path + " : ", ke);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zce) {
            throw new IOException("Error writing max ledger sequence number " + ledgerSeqNo + " to "
                    + path + " : ", zce);
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Error writing max ledger sequence number " + ledgerSeqNo + " to "
                    + path + " : ", e);
        }
    }

    static long toLedgerSequenceNo(byte[] data) {
        String seqNoStr = new String(data, UTF_8);
        return Long.valueOf(seqNoStr);
    }

    static byte[] toBytes(long ledgerSeqNo) {
        return Long.toString(ledgerSeqNo).getBytes(UTF_8);
    }
}
