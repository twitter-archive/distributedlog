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
package com.twitter.distributedlog.util;

import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utilities about DL implementations like uri, log segments, metadata serialization and deserialization.
 */
public class DLUtils {

    static final Logger logger = LoggerFactory.getLogger(DLUtils.class);

    /**
     * Extract zk servers fro dl <i>uri</i>.
     *
     * @param uri
     *          dl uri
     * @return zk servers
     */
    public static String getZKServersFromDLUri(URI uri) {
        return uri.getAuthority().replace(";", ",");
    }

    /**
     * Find the log segment whose transaction ids are not less than provided <code>transactionId</code>.
     *
     * @param segments
     *          segments to search
     * @param transactionId
     *          transaction id to find
     * @return the first log segment whose transaction ids are not less than <code>transactionId</code>.
     */
    public static int findLogSegmentNotLessThanTxnId(List<LogSegmentMetadata> segments,
                                                     long transactionId) {
        int found = -1;
        for (int i = segments.size() - 1; i >= 0; i--) {
            LogSegmentMetadata segment = segments.get(i);
            if (segment.getFirstTxId() <= transactionId) {
                found = i;
                break;
            }
        }
        if (found <= -1) {
            return -1;
        }
        if (found == 0 && segments.get(0).getFirstTxId() == transactionId) {
            return 0;
        }
        LogSegmentMetadata foundSegment = segments.get(found);
        if (foundSegment.getFirstTxId() == transactionId) {
            for (int i = found - 1; i >= 0; i--) {
                LogSegmentMetadata segment = segments.get(i);
                if (segment.isInProgress()) {
                    break;
                }
                if (segment.getLastTxId() < transactionId) {
                    break;
                }
                found = i;
            }
            return found;
        } else {
            if (foundSegment.isInProgress()
                    || found == segments.size() - 1) {
                return found;
            }
            if (foundSegment.getLastTxId() >= transactionId) {
                return found;
            }
            return found + 1;
        }
    }

    /**
     * Assign next log segment sequence number based on a decreasing list of log segments.
     *
     * @param segmentListDesc
     *          a decreasing list of log segments
     * @return null if no log segments was assigned a sequence number in <code>segmentListDesc</code>.
     *         otherwise, return next log segment sequence number
     */
    public static Long nextLogSegmentSequenceNumber(List<LogSegmentMetadata> segmentListDesc) {
        int lastAssignedLogSegmentIdx = -1;
        Long lastAssignedLogSegmentSeqNo = null;
        Long nextLogSegmentSeqNo = null;

        for (int i = 0; i < segmentListDesc.size(); i++) {
            LogSegmentMetadata metadata = segmentListDesc.get(i);
            if (LogSegmentMetadata.supportsLogSegmentSequenceNo(metadata.getVersion())) {
                lastAssignedLogSegmentSeqNo = metadata.getLogSegmentSequenceNumber();
                lastAssignedLogSegmentIdx = i;
                break;
            }
        }

        if (null != lastAssignedLogSegmentSeqNo) {
            // latest log segment is assigned with a sequence number, start with next sequence number
            nextLogSegmentSeqNo = lastAssignedLogSegmentSeqNo + lastAssignedLogSegmentIdx + 1;
        }
        return nextLogSegmentSeqNo;
    }

    /**
     * Compute the start sequence id for <code>segment</code>, based on previous segment list
     * <code>segmentListDesc</code>.
     *
     * @param logSegmentDescList
     *          list of segments in descending order
     * @param segment
     *          segment to compute start sequence id for
     * @return start sequence id
     */
    public static long computeStartSequenceId(List<LogSegmentMetadata> logSegmentDescList,
                                              LogSegmentMetadata segment)
            throws UnexpectedException {
        long startSequenceId = 0L;
        for (LogSegmentMetadata metadata : logSegmentDescList) {
            if (metadata.getLogSegmentSequenceNumber() >= segment.getLogSegmentSequenceNumber()) {
                continue;
            } else if (metadata.getLogSegmentSequenceNumber() < (segment.getLogSegmentSequenceNumber() - 1)) {
                break;
            }
            if (metadata.isInProgress()) {
                throw new UnexpectedException("Should not complete log segment " + segment.getLogSegmentSequenceNumber()
                        + " since it's previous log segment is still inprogress : " + logSegmentDescList);
            }
            if (metadata.supportsSequenceId()) {
                startSequenceId = metadata.getStartSequenceId() + metadata.getRecordCount();
            }
        }
        return startSequenceId;
    }

    /**
     * Deserialize log segment sequence number for bytes <code>data</code>.
     *
     * @param data
     *          byte representation of log segment sequence number
     * @return log segment sequence number
     * @throws NumberFormatException if the bytes aren't valid
     */
    public static long deserializeLogSegmentSequenceNumber(byte[] data) {
        String seqNoStr = new String(data, UTF_8);
        return Long.parseLong(seqNoStr);
    }

    /**
     * Serilize log segment sequence number <code>logSegmentSeqNo</code> into bytes.
     *
     * @param logSegmentSeqNo
     *          log segment sequence number
     * @return byte representation of log segment sequence number
     */
    public static byte[] serializeLogSegmentSequenceNumber(long logSegmentSeqNo) {
        return Long.toString(logSegmentSeqNo).getBytes(UTF_8);
    }

    /**
     * Deserialize log record transaction id for bytes <code>data</code>.
     *
     * @param data
     *          byte representation of log record transaction id
     * @return log record transaction id
     * @throws NumberFormatException if the bytes aren't valid
     */
    public static long deserializeTransactionId(byte[] data) {
        String seqNoStr = new String(data, UTF_8);
        return Long.parseLong(seqNoStr);
    }

    /**
     * Serilize log record transaction id <code>transactionId</code> into bytes.
     *
     * @param transactionId
     *          log record transaction id
     * @return byte representation of log record transaction id.
     */
    public static byte[] serializeTransactionId(long transactionId) {
        return Long.toString(transactionId).getBytes(UTF_8);
    }

    /**
     * Serialize ledger id into bytes.
     *
     * @param ledgerId
     *          ledger id
     * @return bytes representation of ledger id
     */
    public static byte[] ledgerId2Bytes(long ledgerId) {
        return Long.toString(ledgerId).getBytes(UTF_8);
    }

    /**
     * Deserialize bytes into ledger id.
     *
     * @param data
     *          bytes representation of ledger id
     * @return ledger id
     */
    public static long bytes2LedgerId(byte[] data) {
        return Long.parseLong(new String(data, UTF_8));
    }
}
