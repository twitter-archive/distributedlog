package com.twitter.distributedlog.v2;

import java.io.Closeable;
import java.io.IOException;

public interface MetadataAccessor extends Closeable {
    public void createOrUpdateMetadata(byte[] metadata) throws IOException;

    public void deleteMetadata() throws IOException;

    public byte[] getMetadata() throws IOException;

    /**
     * Close the distributed log metadata, freeing any resources it may hold.
     */
    public void close() throws IOException;

}
