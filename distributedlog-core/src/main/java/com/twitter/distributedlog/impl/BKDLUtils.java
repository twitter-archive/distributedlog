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
        String reason = null;
        char chars[] = nameOfStream.toCharArray();
        char c;
        // validate the stream to see if meet zookeeper path's requirement
        for (int i = 0; i < chars.length; i++) {
            c = chars[i];

            if (c == 0) {
                reason = "null character not allowed @" + i;
                break;
            } else if (c == '/') {
                reason = "'/' not allowed @" + i;
                break;
            } else if (c > '\u0000' && c < '\u001f'
                    || c > '\u007f' && c < '\u009F'
                    || c > '\ud800' && c < '\uf8ff'
                    || c > '\ufff0' && c < '\uffff') {
                reason = "invalid charater @" + i;
                break;
            }
        }
        if (null != reason) {
            throw new InvalidStreamNameException(nameOfStream, reason);
        }
        if (isReservedStreamName(nameOfStream)) {
            throw new InvalidStreamNameException(nameOfStream,
                    "Stream Name is reserved");
        }
    }
}
