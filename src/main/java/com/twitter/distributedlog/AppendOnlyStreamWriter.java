package com.twitter.distributedlog;

import java.io.Closeable;
import java.io.IOException;

public class AppendOnlyStreamWriter implements Closeable {
    private BKUnPartitionedSyncLogWriter logwriter;
    private long currentPos;

    public AppendOnlyStreamWriter(BKUnPartitionedSyncLogWriter logWriter, long position) {
        this.logwriter = logWriter;
        this.currentPos = position;
    }

    public void write(byte[] data) throws IOException {
        currentPos += data.length;
        logwriter.write(new LogRecord(currentPos, data));
    }

    public void force(boolean metadata) throws IOException {
        logwriter.setReadyToFlush();
        logwriter.flushAndSync();
    }

    public long position() {
        return currentPos;
    }

    @Override
    public void close() throws IOException {
        logwriter.closeAndComplete();
    }
}
