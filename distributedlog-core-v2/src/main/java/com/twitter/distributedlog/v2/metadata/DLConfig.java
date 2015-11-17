package com.twitter.distributedlog.v2.metadata;

import java.io.IOException;

/**
 * Specific config of a given implementation of DL
 */
public interface DLConfig {
    /**
     * Serialize the dl config into a string.
     */
    public String serialize();

    /**
     * Deserialize the dl config from a readable stream.
     *
     * @param data
     *          bytes to desrialize dl config.
     * @throws IOException if fail to deserialize the dl config string representation.
     */
    public void deserialize(byte[] data) throws IOException;
}
