package com.twitter.distributedlog.service.stream;

import com.google.common.base.Optional;
import com.twitter.util.Future;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage lifecycle of streams.
 */
public interface StreamManager {

    /**
     * Get a cached stream, returning null if it doesnt exist.
     * @param stream name
     * @return the cached stream
     */
    Stream getStream(String stream);

    /**
     * Get a cached stream and create a new one if it doesnt exist.
     * @param stream name
     * @return future satisfied once close complete
     */
    Stream openStream(String stream) throws IOException;

    /**
     * Asynchronous close method.
     * @param stream being released
     */
    void notifyReleased(Stream stream);

    /**
     * Asynchronous close method.
     * @param stream being acquired
     */
    void notifyAcquired(Stream stream);

    /**
     * Asynchronous close method.
     * @param stream being uncached
     * @return whether the stream existed or not
     */
    boolean notifyRemoved(Stream stream);

    /**
     * Asynchronous delete method.
     * @param stream name
     * @return future satisfied once delete complete
     */
    Future<Void> deleteAndRemoveAsync(String streamName);

    /**
     * Asynchronous close and uncache method.
     * @param stream name
     * @return future satisfied once close and uncache complete
     */
    Future<Void> closeAndRemoveAsync(String streamName);

    /**
     * Close and uncache after delayMs.
     * @param stream to remove
     */
    void scheduleRemoval(Stream stream, long delayMs);

    /**
     * Close all stream.
     * @return future satisfied all streams closed
     */
    Future<List<Void>> closeStreams();

    /**
     * Return map with stream ownership info.
     * @param regex for filtering streams
     * @return map containing ownership info
     */
    Map<String, String> getStreamOwnershipMap(Optional<String> regex);

    /**
     * Just return cached all streams.
     * @return map containing all cached streams
     */
    ConcurrentHashMap<String, Stream> getCachedStreams();

    /**
     * Just return cached all acquired streams.
     * @return map containing all acquired streams
     */
    ConcurrentHashMap<String, Stream> getAcquiredStreams();

    /**
     * Close manager and disallow further activity.
     */
    void close();
}
