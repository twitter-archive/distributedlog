package com.twitter.distributedlog.v2.util;

import java.net.URI;

public class DLUtils {

    /**
     * Extract zk servers fro dl <i>uri</i>.
     *
     * @param uri
     *          dl uri
     * @return zk servers
     */
    public static String getZKServersFromDLUri(URI uri) {
        return uri.getAuthority().replace(";", ",");
    }
}
