package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public class BKUnPartitionedLogWriterBase extends BKBaseLogWriter {
    private BKPerStreamLogWriter perStreamWriter = null;
    private BKLogPartitionWriteHandler partitionHander = null;


    public BKUnPartitionedLogWriterBase(DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) throws IOException {
        super(conf, bkdlm);
    }

    @Override
    protected BKLogPartitionWriteHandler getCachedPartitionHandler(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        return partitionHander;
    }

    // Since we have only one stream we simply maintain one partition handler and one ledger
    @Override
    protected void cachePartitionHandler(String streamIdentifier, BKLogPartitionWriteHandler ledgerHandler) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        partitionHander = ledgerHandler;
    }

    @Override
    protected BKLogPartitionWriteHandler removeCachedPartitionHandler(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        BKLogPartitionWriteHandler ret = partitionHander;
        partitionHander = null;
        return ret;
    }

    @Override
    protected Collection<BKLogPartitionWriteHandler> getCachedPartitionHandlers() {
        LinkedList<BKLogPartitionWriteHandler> list = new LinkedList<BKLogPartitionWriteHandler>();
        if (null != partitionHander) {
            list.add(partitionHander);
        }
        return list;
    }

    @Override
    protected BKPerStreamLogWriter getCachedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        return perStreamWriter;
    }

    @Override
    protected void cacheLogWriter(String streamIdentifier, BKPerStreamLogWriter logWriter) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        perStreamWriter = logWriter;
    }

    @Override
    protected BKPerStreamLogWriter removeCachedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        BKPerStreamLogWriter ret = perStreamWriter;
        perStreamWriter = null;
        return ret;
    }

    @Override
    protected Collection<BKPerStreamLogWriter> getCachedLogWriters() {
        LinkedList<BKPerStreamLogWriter> list = new LinkedList<BKPerStreamLogWriter>();
        if (null != perStreamWriter) {
            list.add(perStreamWriter);
        }
        return list;
    }

    public void closeAndComplete() throws IOException {
        if (null != perStreamWriter && null != partitionHander) {
            try {
                waitForTruncation();
                Map.Entry<Long, DLSN> lastPoint = perStreamWriter.closeToFinalize();
                partitionHander.completeAndCloseLogSegment(lastPoint.getKey(), lastPoint.getValue().getEntryId(), lastPoint.getValue().getSlotId());
            } finally {
                // ensure partition handler is closed.
                partitionHander.close();
            }
            perStreamWriter = null;
            partitionHander = null;
        }
        close();
    }

}
