package com.twitter.distributedlog.impl;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;

import java.net.URI;

/**
 * Utils for bookkeeper based distributedlog implementation
 */
public class BKDLUtils {

    /**
     * Is it a reserved stream name in bkdl namespace?
     *
     * @param name
     *          stream name
     * @return true if it is reserved name, otherwise false.
     */
    public static boolean isReservedStreamName(String name) {
        return name.startsWith(".");
    }

    /**
     * Validate the configuration and uri.
     *
     * @param conf
     *          distributedlog configuration
     * @param uri
     *          distributedlog uri
     * @throws IllegalArgumentException
     */
    public static void validateConfAndURI(DistributedLogConfiguration conf, URI uri)
        throws IllegalArgumentException {
        if (null == conf) {
            throw new IllegalArgumentException("Incorrect Configuration");
        }
        if ((null == uri) || (null == uri.getAuthority()) || (null == uri.getPath())) {
            throw new IllegalArgumentException("Incorrect ZK URI");
        }
    }

    /**
     * Validate the stream name.
     *
     * @param nameOfStream
     *          name of stream
     * @throws InvalidStreamNameException
     */
    public static void validateName(String nameOfStream)
            throws InvalidStreamNameException {
        if (nameOfStream.contains("/")) {
            throw new InvalidStreamNameException(nameOfStream,
                    "Stream Name contains invalid characters");
        }
        if (isReservedStreamName(nameOfStream)) {
            throw new InvalidStreamNameException(nameOfStream,
                    "Stream Name is reserved");
        }
    }
}
