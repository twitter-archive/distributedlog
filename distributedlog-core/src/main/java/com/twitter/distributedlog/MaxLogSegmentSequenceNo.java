/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.util.DLUtils;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility class for storing and reading max ledger sequence number
 */
class MaxLogSegmentSequenceNo {

    static final Logger LOG = LoggerFactory.getLogger(MaxLogSegmentSequenceNo.class);

    Version version;
    long maxSeqNo;

    MaxLogSegmentSequenceNo(Versioned<byte[]> logSegmentsData) {
        if (null != logSegmentsData
                && null != logSegmentsData.getValue()
                && null != logSegmentsData.getVersion()) {
            version = logSegmentsData.getVersion();
            try {
                maxSeqNo = DLUtils.deserializeLogSegmentSequenceNumber(logSegmentsData.getValue());
            } catch (NumberFormatException nfe) {
                maxSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
            }
        } else {
            maxSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
            if (null != logSegmentsData && null != logSegmentsData.getVersion()) {
                version = logSegmentsData.getVersion();
            } else {
                version = new ZkVersion(-1);
            }
        }
    }

    synchronized int getZkVersion() {
        return ((ZkVersion) version).getZnodeVersion();
    }

    synchronized long getSequenceNumber() {
        return maxSeqNo;
    }

    synchronized MaxLogSegmentSequenceNo update(int zkVersion, long logSegmentSeqNo) {
        return update(new ZkVersion(zkVersion), logSegmentSeqNo);
    }

    synchronized MaxLogSegmentSequenceNo update(ZkVersion version, long logSegmentSeqNo) {
        if (version.compare(this.version) == Version.Occurred.AFTER) {
            this.version = version;
            this.maxSeqNo = logSegmentSeqNo;
        }
        return this;
    }

    synchronized void store(ZooKeeperClient zkc, String path, long logSegmentSeqNo) throws IOException {
        try {
            Stat stat = zkc.get().setData(path,
                    DLUtils.serializeLogSegmentSequenceNumber(logSegmentSeqNo), getZkVersion());
            update(stat.getVersion(), logSegmentSeqNo);
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

}
