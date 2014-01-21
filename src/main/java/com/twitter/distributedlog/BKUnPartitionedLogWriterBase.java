package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import com.twitter.distributedlog.util.Pair;

abstract class BKUnPartitionedLogWriterBase extends BKBaseLogWriter {
    private BKPerStreamLogWriter perStreamWriter = null;
    private BKPerStreamLogWriter allocatedPerStreamWriter = null;
    private BKLogPartitionWriteHandler partitionHander = null;

    public BKUnPartitionedLogWriterBase(DistributedLogConfiguration conf, BKDistributedLogManager bkdlm) {
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

    @Override
    protected BKPerStreamLogWriter getAllocatedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        return allocatedPerStreamWriter;
    }

    @Override
    protected void cacheAllocatedLogWriter(String streamIdentifier, BKPerStreamLogWriter logWriter) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        allocatedPerStreamWriter = logWriter;
    }

    @Override
    protected BKPerStreamLogWriter removeAllocatedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(DistributedLogConstants.DEFAULT_STREAM));
        try {
            return allocatedPerStreamWriter;
        } finally {
            allocatedPerStreamWriter = null;
        }
    }

    @Override
    protected Collection<BKPerStreamLogWriter> getAllocatedLogWriters() {
        LinkedList<BKPerStreamLogWriter> list = new LinkedList<BKPerStreamLogWriter>();
        if (null != allocatedPerStreamWriter) {
            list.add(allocatedPerStreamWriter);
        }
        return list;
    }

    public void closeAndComplete() throws IOException {
        if (null != perStreamWriter && null != partitionHander) {
            try {
                waitForTruncation();
                partitionHander.completeAndCloseLogSegment(perStreamWriter);
            } finally {
                // ensure partition handler is closed.
                partitionHander.close();
            }
            perStreamWriter = null;
            partitionHander = null;
        }
        close();
    }

    /**
     * Close the stream without necessarily flushing immediately.
     * This may be called if the stream is in error such as after a
     * previous write or close threw an exception.
     */
    public void abort() throws IOException {
        if (null != perStreamWriter) {
            perStreamWriter.abort();
            perStreamWriter = null;
        }

        close();
    }
}
