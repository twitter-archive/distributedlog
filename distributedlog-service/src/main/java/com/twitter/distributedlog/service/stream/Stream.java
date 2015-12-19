package com.twitter.distributedlog.service.stream;

import com.twitter.util.Future;
import java.io.IOException;

/**
 * Stream is the per stream request handler in the DL service layer. The collection of Streams in
 * the proxy are managed by StreamManager.
 */
public interface Stream {

    /**
     * Get the stream's last recorded current owner (may be out of date). Used
     * as a hint for the client.
     * @param last known owner for the stream
     */
    String getOwner();

    /**
     * Get the stream name.
     * @param stream name
     */
    String getStreamName();

    /**
     * Expensive initialization code run after stream has been allocated in
     * StreamManager.
     */
    void initialize() throws IOException;

    /**
     * Another initialize method (actually Thread.start). Should probably be
     * moved to initiaize().
     */
    void start();

    /**
     * Asynchronous close method.
     * @param reason for closing
     * @return future satisfied once close complete
     */
    Future<Void> requestClose(String reason);

    /**
     * Delete the stream from DL backend.
     */
    void delete() throws IOException;

    /**
     * Execute the stream operation against this stream.
     * @param stream operation to execute
     */
    void submit(StreamOp op);
}
