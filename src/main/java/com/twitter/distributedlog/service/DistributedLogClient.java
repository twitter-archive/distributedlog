package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DLSN;
import com.twitter.util.Future;

import java.nio.ByteBuffer;

public interface DistributedLogClient {
    /**
     * Write <i>data</i> to a given <i>stream</i>.
     *
     * @param stream
     *          Stream Name.
     * @param data
     *          Data to write.
     * @return a future representing a sequence id returned for this write.
     */
    Future<DLSN> write(String stream, ByteBuffer data);

    /**
     * Close the client.
     */
    void close();
}
