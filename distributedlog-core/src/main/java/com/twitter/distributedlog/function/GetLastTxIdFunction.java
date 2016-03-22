package com.twitter.distributedlog.function;

import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LogSegmentMetadata;
import scala.runtime.AbstractFunction1;

import java.util.List;

/**
 * Retrieve the last tx id from list of log segments
 */
public class GetLastTxIdFunction extends AbstractFunction1<List<LogSegmentMetadata>, Long> {

    public static final GetLastTxIdFunction INSTANCE = new GetLastTxIdFunction();

    private GetLastTxIdFunction() {}

    @Override
    public Long apply(List<LogSegmentMetadata> segmentList) {
        long lastTxId = DistributedLogConstants.INVALID_TXID;
        for (LogSegmentMetadata l : segmentList) {
            lastTxId = Math.max(lastTxId, l.getLastTxId());
        }
        return lastTxId;
    }
}
