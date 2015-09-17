package com.twitter.distributedlog.impl;

import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.logsegment.LogSegmentFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Filters based on current zookeeper log segments.
 */
public class ZKLogSegmentFilters {

    static final Logger LOG = LoggerFactory.getLogger(ZKLogSegmentFilters.class);

    /**
     * Write handler filter should return all inprogress log segments and the last completed log segment.
     * Because sequence id & ledger sequence number assignment rely on previous log segments.
     */
    public static final LogSegmentFilter WRITE_HANDLE_FILTER = new LogSegmentFilter() {
        @Override
        public Collection<String> filter(Collection<String> fullList) {
            List<String> result = new ArrayList<String>(fullList.size());
            String lastCompletedLogSegmentName = null;
            long lastLogSegmentSequenceNumber = -1L;
            for (String s : fullList) {
                if (s.startsWith(DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX)) {
                    result.add(s);
                } else if (s.startsWith(DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX)) {
                    String[] parts = s.split("_");
                    try {
                        if (2 == parts.length) {
                            // name: logrecs_<logsegment_sequence_number>
                            long logSegmentSequenceNumber = Long.parseLong(parts[1]);
                            if (logSegmentSequenceNumber > lastLogSegmentSequenceNumber) {
                                lastLogSegmentSequenceNumber = logSegmentSequenceNumber;
                                lastCompletedLogSegmentName = s;
                            }
                        } else if (6 == parts.length) {
                            // name: logrecs_<start_tx_id>_<end_tx_id>_<logsegment_sequence_number>_<ledger_id>_<region_id>
                            long logSegmentSequenceNumber = Long.parseLong(parts[3]);
                            if (logSegmentSequenceNumber > lastLogSegmentSequenceNumber) {
                                lastLogSegmentSequenceNumber = logSegmentSequenceNumber;
                                lastCompletedLogSegmentName = s;
                            }
                        } else {
                            // name: logrecs_<start_tx_id>_<end_tx_id> or any unknown names
                            // we don't know the ledger sequence from the name, so add it to the list
                            result.add(s);
                        }
                    } catch (NumberFormatException nfe) {
                        LOG.warn("Unexpected sequence number in log segment {} :", s, nfe);
                        result.add(s);
                    }
                } else {
                    LOG.error("Unknown log segment name : {}", s);
                }
            }
            if (null != lastCompletedLogSegmentName) {
                result.add(lastCompletedLogSegmentName);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Filtered log segments {} from {}.", result, fullList);
            }
            return result;
        }
    };

}
