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
class MaxLogSegmentSequenceNo {

    static final Logger LOG = LoggerFactory.getLogger(MaxLogSegmentSequenceNo.class);

    int zkVersion;
    long maxSeqNo;

    MaxLogSegmentSequenceNo(DataWithStat dataWithStat) {
        if (dataWithStat.exists()) {
            zkVersion = dataWithStat.getStat().getVersion();
            try {
                maxSeqNo = toLogSegmentSequenceNo(dataWithStat.getData());
            } catch (NumberFormatException nfe) {
                maxSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
            }
        } else {
            maxSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
            zkVersion = -1;
        }
    }

    synchronized int getZkVersion() {
        return zkVersion;
    }

    synchronized long getSequenceNumber() {
        return maxSeqNo;
    }

    synchronized MaxLogSegmentSequenceNo update(int zkVersion, long logSegmentSeqNo) {
        this.zkVersion = zkVersion;
        this.maxSeqNo = logSegmentSeqNo;
        return this;
    }

    synchronized void store(ZooKeeperClient zkc, String path, long logSegmentSeqNo) throws IOException {
        try {
            Stat stat = zkc.get().setData(path, toBytes(logSegmentSeqNo), zkVersion);
            this.zkVersion = stat.getVersion();
            this.maxSeqNo = logSegmentSeqNo;
        } catch (KeeperException ke) {
            throw new ZKException("Error writing max ledger sequence number " + logSegmentSeqNo + " to "
                                  + path + " : ", ke);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zce) {
            throw new IOException("Error writing max ledger sequence number " + logSegmentSeqNo + " to "
                    + path + " : ", zce);
        } catch (InterruptedException e) {
            throw new DLInterruptedException("Error writing max ledger sequence number " + logSegmentSeqNo + " to "
                    + path + " : ", e);
        }
    }

    static long toLogSegmentSequenceNo(byte[] data) {
        String seqNoStr = new String(data, UTF_8);
        return Long.valueOf(seqNoStr);
    }

    static byte[] toBytes(long logSegmentSeqNo) {
        return Long.toString(logSegmentSeqNo).getBytes(UTF_8);
    }
}
