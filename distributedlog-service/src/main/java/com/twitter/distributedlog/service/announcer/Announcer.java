package com.twitter.distributedlog.service.announcer;

import java.io.IOException;

/**
 * Announce service information
 */
public interface Announcer {

    /**
     * Announce service info.
     */
    void announce() throws IOException;

    /**
     * Unannounce the service info.
     */
    void unannounce() throws IOException;

    /**
     * Close the announcer.
     */
    void close();
}
