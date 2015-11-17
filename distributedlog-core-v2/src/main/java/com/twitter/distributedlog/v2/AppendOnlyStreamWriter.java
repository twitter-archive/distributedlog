package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.LogRecord;

import java.io.Closeable;
import java.io.IOException;

public class AppendOnlyStreamWriter implements Closeable {
    private BKUnPartitionedSyncLogWriter logWriter;
    private long currentPos;

    public AppendOnlyStreamWriter(BKUnPartitionedSyncLogWriter logWriter, long position) {
        this.logWriter = logWriter;
        this.currentPos = position;
    }

    public void write(byte[] data) throws IOException {
        currentPos += data.length;
        logWriter.write(new LogRecord(currentPos, data));
    }

    public void force(boolean metadata) throws IOException {
        logWriter.setReadyToFlush();
        logWriter.flushAndSync();
    }

    public long position() {
        return currentPos;
    }

    @Override
    public void close() throws IOException {
        logWriter.closeAndComplete();
    }
}
