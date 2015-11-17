package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.LogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogConfiguration extends com.twitter.distributedlog.DistributedLogConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogConfiguration.class);

    // default layout version for DL 0.2.* release
    public static final int BKDL_LEDGER_METADATA_LAYOUT_VERSION_V2_DEFAULT =
            LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V1_ORIGINAL.value;

    public DistributedLogConfiguration() {
        super();
    }

    /**
     * Get DL ledger metadata output layout version
     *
     * @return layout version
     */
    public int getDLLedgerMetadataLayoutVersion() {
        return this.getInt(BKDL_LEDGER_METADATA_LAYOUT_VERSION,
                BKDL_LEDGER_METADATA_LAYOUT_VERSION_V2_DEFAULT);
    }

}
