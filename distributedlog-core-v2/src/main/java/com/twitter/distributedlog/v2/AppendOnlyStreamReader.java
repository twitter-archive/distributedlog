package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.LogRecord;

import java.io.IOException;
import java.io.InputStream;


public class AppendOnlyStreamReader extends InputStream {
    private InputStream payloadStream = null;
    private final DistributedLogManager dlm;
    private LogReader reader;
    private long currentPosition;
    private static final int SKIP_BUFFER_SIZE = 512;

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
    private InputStream nextStream() throws IOException {
        LogRecord record = reader.readNext(false);
        if (null != record) {
            return record.getPayLoadInputStream();
        } else {
            record = reader.readNext(false);
            if (null != record) {
                return record.getPayLoadInputStream();
            } else {
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
        if (payloadStream == null) {
            payloadStream = nextStream();
            if (payloadStream == null) {
                return read;
            }
        }

        while (read < len) {
            int thisread = payloadStream.read(b, off + read, (len - read));
            if (thisread == -1) {
                payloadStream = nextStream();
                if (payloadStream == null) {
                    return read;
                }
            } else {
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
