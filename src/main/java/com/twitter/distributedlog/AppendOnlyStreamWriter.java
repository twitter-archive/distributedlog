package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendOnlyStreamWriter implements Closeable {
    static final Logger LOG = LoggerFactory.getLogger(AppendOnlyStreamWriter.class);

    // Use a 1-length array to satisfy Java's inner class reference rules. Use primitive
    // type because synchronized block is needed anyway.
    final long[] syncPos = new long[1];
    BKUnPartitionedAsyncLogWriter logWriter;
    long requestPos = 0;

    public AppendOnlyStreamWriter(BKUnPartitionedAsyncLogWriter logWriter, long pos) {
        LOG.debug("initialize at position {}", pos);
        this.logWriter = logWriter;
        this.syncPos[0] = pos;
        this.requestPos = pos;
    }

    public Future<DLSN> write(byte[] data) {
        requestPos += data.length;
        Future<DLSN> writeResult = logWriter.write(new LogRecord(requestPos, data));
        return writeResult.addEventListener(new WriteCompleteListener(requestPos));
    }

    public void force(boolean metadata) throws IOException {
        long pos = 0;
        try {
            pos = Await.result(logWriter.flushAndSyncAll()).longValue();
        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception ex) {
            LOG.error("unexpected exception in AppendOnlyStreamWriter.force ", ex);
            throw new UnexpectedException("unexpected exception in AppendOnlyStreamWriter.force", ex);
        }
        synchronized (syncPos) {
            syncPos[0] = pos;
        }
    }

    public long position() {
        synchronized (syncPos) {
            return syncPos[0];
        }
    }

    @Override
    public void close() throws IOException {
        logWriter.closeAndComplete();
    }

    class WriteCompleteListener implements FutureEventListener<DLSN> {
        private final long position;
        public WriteCompleteListener(long position) {
            this.position = position;
        }
        @Override
        public void onSuccess(DLSN response) {
            synchronized (syncPos) {
                if (position > syncPos[0]) {
                    syncPos[0] = position;
                }
            }
        }
        @Override
        public void onFailure(Throwable cause) {
            // Handled at the layer above
        }
    }
}
