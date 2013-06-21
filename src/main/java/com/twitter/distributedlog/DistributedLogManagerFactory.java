package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;

public class DistributedLogManagerFactory {
    public static DistributedLogManager createDistributedLogManager(String name, URI uri) throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, new DistributedLogConfiguration(), uri);
    }


    public static DistributedLogManager createDistributedLogManager(String name,
                                                                    DistributedLogConfiguration conf, URI uri) throws IOException, IllegalArgumentException {
        // input validation
        //
        if (null == conf) {
            throw new IllegalArgumentException("Incorrect Configuration");
        }

        if ((null == uri) || (null == uri.getAuthority()) || (null == uri.getPath())) {
            throw new IllegalArgumentException("Incorrect ZK URI");
        }

        return new BKDistributedLogManager(name, conf, uri);
    }

}
