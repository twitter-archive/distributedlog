package com.twitter.distributedlog;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendOnlyStreamReader extends InputStream {
    static final Logger LOG = LoggerFactory.getLogger(AppendOnlyStreamReader.class);

    private LogRecordWithInputStream currentLogRecord = null;
    private final DistributedLogManager dlm;
    private LogReader reader;
    private long currentPosition;
    private static final int SKIP_BUFFER_SIZE = 512;

    // Cache the input stream for a log record.
    private static class LogRecordWithInputStream {
        private final InputStream payloadStream;
        private final LogRecordWithDLSN logRecord;

        LogRecordWithInputStream(LogRecordWithDLSN logRecord) {
            Preconditions.checkNotNull(logRecord);

            LOG.debug("Got record dlsn = {}, txid = {}, len = {}",
                new Object[] {logRecord.getDlsn(), logRecord.getTransactionId(), logRecord.getPayload().length});

            this.logRecord = logRecord;
            this.payloadStream = logRecord.getPayLoadInputStream();
        }

        InputStream getPayLoadInputStream() {
            return payloadStream;
        }

        LogRecordWithDLSN getLogRecord() {
            return logRecord;
        }
    }

    /**
     * Construct ledger input stream
     *
     * @param dlm the Distributed Log Manager to access the stream
     */
    AppendOnlyStreamReader(DistributedLogManager dlm)
        throws IOException {
        this.dlm = dlm;
        reader = dlm.getInputStream(0);
        currentPosition = 0;
    }

    /**
     * Get input stream representing next entry in the
     * ledger.
     *
     * @return input stream, or null if no more entries
     */
    private LogRecordWithInputStream nextStream() throws IOException {
        LogRecordWithDLSN record = reader.readNext(false);

        if (null != record) {
            return new LogRecordWithInputStream(record);
        } else {
            record = reader.readNext(false);
            if (null != record) {
                return new LogRecordWithInputStream(record);
            } else {
                LOG.debug("No record");
                return null;
            }
        }
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        if (read(b, 0, 1) != 1) {
            return -1;
        } else {
            return b[0];
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = 0;
        if (currentLogRecord == null) {
            currentLogRecord = nextStream();
            if (currentLogRecord == null) {
                return read;
            }
        }

        while (read < len) {
            int thisread = currentLogRecord.getPayLoadInputStream().read(b, off + read, (len - read));
            if (thisread == -1) {
                currentLogRecord = nextStream();
                if (currentLogRecord == null) {
                    return read;
                }
            } else {
                LOG.debug("Offset saved = {}, persisted = {}",
                    currentPosition, currentLogRecord.getLogRecord().getTransactionId());
                currentPosition += thisread;
                read += thisread;
            }
        }
        return read;
    }

    public boolean skipTo(long position) throws IOException {
        // Allocate once and reuse unlike the default implementation
        // in InputStream
        byte[] skipBuffer = new byte[SKIP_BUFFER_SIZE];

        // Underlying reader doesnt support positioning
        // beyond the current point in the stream
        // So we should reset the DLog Reader and reposition a new one
        // In practice positioning behind the current point is not
        // a frequent operation so this is fine
        if (position < currentPosition) {
            reader.close();
            reader = dlm.getInputStream(0);
            currentPosition = 0;
        }

        long read = currentPosition;
        while (read < position) {
            long bytesToRead = Math.min(position - read, SKIP_BUFFER_SIZE);
            int bytesRead = read(skipBuffer, 0 , (int)bytesToRead);
            if (bytesRead < bytesToRead) {
                return false;
            }
            read += bytesToRead;
        }
        return true;
    }

    public long position() {
        return currentPosition;
    }
}
