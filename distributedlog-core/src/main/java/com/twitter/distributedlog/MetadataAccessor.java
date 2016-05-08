package com.twitter.distributedlog;

import com.twitter.distributedlog.io.AsyncCloseable;

import java.io.Closeable;
import java.io.IOException;

public interface MetadataAccessor extends Closeable, AsyncCloseable {
    /**
     * Get the name of the stream managed by this log manager
     * @return streamName
     */
    public String getStreamName();

    public void createOrUpdateMetadata(byte[] metadata) throws IOException;

    public void deleteMetadata() throws IOException;

    public byte[] getMetadata() throws IOException;

    /**
     * Close the distributed log metadata, freeing any resources it may hold.
     */
    public void close() throws IOException;

}
