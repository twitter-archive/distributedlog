package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;

abstract class BKUnPartitionedLogWriterBase extends BKBaseLogWriter {
    protected BKLogSegmentWriter perStreamWriter = null;
    protected BKLogSegmentWriter allocatedPerStreamWriter = null;
    protected BKLogPartitionWriteHandler partitionHander = null;

    public BKUnPartitionedLogWriterBase(DistributedLogConfiguration conf, DynamicDistributedLogConfiguration dynConf,
                                        BKDistributedLogManager bkdlm) {
        super(conf, dynConf, bkdlm);
    }

    @Override
    protected BKLogPartitionWriteHandler getCachedPartitionHandler(String streamIdentifier) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        return partitionHander;
    }

    // Since we have only one stream we simply maintain one partition handler and one ledger
    @Override
    protected void cachePartitionHandler(String streamIdentifier, BKLogPartitionWriteHandler ledgerHandler) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        partitionHander = ledgerHandler;
    }

    @Override
    protected BKLogPartitionWriteHandler removeCachedPartitionHandler(String streamIdentifier) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
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
    protected BKLogSegmentWriter getCachedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        return perStreamWriter;
    }

    @Override
    protected void cacheLogWriter(String streamIdentifier, BKLogSegmentWriter logWriter) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        perStreamWriter = logWriter;
    }

    @Override
    protected BKLogSegmentWriter removeCachedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        BKLogSegmentWriter ret = perStreamWriter;
        perStreamWriter = null;
        return ret;
    }

    @Override
    protected Collection<BKLogSegmentWriter> getCachedLogWriters() {
        LinkedList<BKLogSegmentWriter> list = new LinkedList<BKLogSegmentWriter>();
        if (null != perStreamWriter) {
            list.add(perStreamWriter);
        }
        return list;
    }

    @Override
    protected BKLogSegmentWriter getAllocatedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        return allocatedPerStreamWriter;
    }

    @Override
    protected void cacheAllocatedLogWriter(String streamIdentifier, BKLogSegmentWriter logWriter) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        allocatedPerStreamWriter = logWriter;
    }

    @Override
    protected BKLogSegmentWriter removeAllocatedLogWriter(String streamIdentifier) {
        assert (streamIdentifier.equals(conf.getUnpartitionedStreamName()));
        try {
            return allocatedPerStreamWriter;
        } finally {
            allocatedPerStreamWriter = null;
        }
    }

    @Override
    protected Collection<BKLogSegmentWriter> getAllocatedLogWriters() {
        LinkedList<BKLogSegmentWriter> list = new LinkedList<BKLogSegmentWriter>();
        if (null != allocatedPerStreamWriter) {
            list.add(allocatedPerStreamWriter);
        }
        return list;
    }

    @VisibleForTesting
    BKLogSegmentWriter getPerStreamWriter() {
        return perStreamWriter;
    }

    @VisibleForTesting
    void closeAndComplete() throws IOException {
        closeAndComplete(true);
    }

    @Override
    protected void closeAndComplete(boolean shouldThrow) throws IOException {
        try {
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
        } catch (IOException exc) {
            LOG.error("Completing Log segments encountered exception", exc);
            if (shouldThrow) {
                throw exc;
            }
        } finally {
            closeNoThrow();
        }
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

        closeNoThrow();
    }
}
