package com.twitter.distributedlog.impl.metadata;

import com.google.common.base.Optional;

import java.net.URI;

/**
 * Log Metadata for Reader
 */
public class ZKLogMetadataForReader extends ZKLogMetadata {

    /**
     * Get the root path to store subscription infos of a log.
     *
     * @param uri
     *          namespace of the log
     * @param logName
     *          name of the log
     * @param logIdentifier
     *          identifier of the log
     * @return subscribers root path
     */
    public static String getSubscribersPath(URI uri, String logName, String logIdentifier) {
        return getLogComponentPath(uri, logName, logIdentifier, SUBSCRIBERS_PATH);
    }

    /**
     * Get the path that stores subscription info for a <code>subscriberId</code> for a <code>log</code>.
     *
     * @param uri
     *          namespace of the log
     * @param logName
     *          name of the log
     * @param logIdentifier
     *          identifier of the log
     * @param subscriberId
     *          subscriber id of the log
     * @return subscriber's path
     */
    public static String getSubscriberPath(URI uri, String logName, String logIdentifier, String subscriberId) {
        return String.format("%s/%s", getSubscribersPath(uri, logName, logIdentifier), subscriberId);
    }

    /**
     * Create a metadata representation of a log for reader.
     *
     * @param uri
     *          namespace to store the log
     * @param logName
     *          name of the log
     * @param logIdentifier
     *          identifier of the log
     * @return metadata representation of a log for reader
     */
    public static ZKLogMetadataForReader of(URI uri, String logName, String logIdentifier) {
        return new ZKLogMetadataForReader(uri, logName, logIdentifier);
    }

    final static String SUBSCRIBERS_PATH = "/subscribers";

    /**
     * metadata representation of a log
     *
     * @param uri           namespace to store the log
     * @param logName       name of the log
     * @param logIdentifier identifier of the log
     */
    private ZKLogMetadataForReader(URI uri, String logName, String logIdentifier) {
        super(uri, logName, logIdentifier);
    }

    /**
     * Get the readlock path for the log or a subscriber of the log.
     *
     * @param subscriberId
     *          subscriber id. it is optional.
     * @return read lock path
     */
    public String getReadLockPath(Optional<String> subscriberId) {
        if (subscriberId.isPresent()) {
            return logRootPath + SUBSCRIBERS_PATH + "/" + subscriberId.get() + READ_LOCK_PATH;
        } else {
            return logRootPath + READ_LOCK_PATH;
        }
    }
}