package com.twitter.distributedlog.acl;

/**
 * Access Control on stream operations
 */
public interface AccessControlManager {

    /**
     * Whether allowing writing to a stream.
     *
     * @param stream
     *          Stream to write
     * @return true if allowing writing to the given stream, otherwise false.
     */
    boolean allowWrite(String stream);

    /**
     * Whether allowing truncating a given stream.
     *
     * @param stream
     *          Stream to truncate
     * @return true if allowing truncating a given stream.
     */
    boolean allowTruncate(String stream);

    /**
     * Whether allowing deleting a given stream.
     *
     * @param stream
     *          Stream to delete
     * @return true if allowing deleting a given stream.
     */
    boolean allowDelete(String stream);

    /**
     * Whether allowing proxies to acquire a given stream.
     *
     * @param stream
     *          stream to acquire
     * @return true if allowing proxies to acquire the given stream.
     */
    boolean allowAcquire(String stream);

    /**
     * Whether allowing proxies to release ownership for a given stream.
     *
     * @param stream
     *          stream to release
     * @return true if allowing proxies to release a given stream.
     */
    boolean allowRelease(String stream);

    /**
     * Close the access control manager.
     */
    void close();
}
