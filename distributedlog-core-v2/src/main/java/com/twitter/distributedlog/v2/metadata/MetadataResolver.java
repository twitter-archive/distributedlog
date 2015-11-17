package com.twitter.distributedlog.v2.metadata;

import java.io.IOException;
import java.net.URI;

/**
 * Resolver to resolve the metadata used to instantiate a DL instance.
 *
 * <p>
 * E.g. we stored a common dl config under /messaging/distributedlog to use
 * bookkeeper cluster x. so all the distributedlog instances under this path
 * inherit this dl config. if a dl D is allocated under /messaging/distributedlog,
 * but use a different cluster y, so its metadata is stored /messaging/distributedlog/D.
 * The resolver resolve the URI
 * </p>
 *
 * <p>
 * The resolver looks up the uri path and tries to interpret the path segments from
 * bottom-to-top to see if there is a DL metadata bound. It stops when it found valid
 * dl metadata.
 * </p>
 */
public interface MetadataResolver {

    /**
     * Resolve the path to get the DL metadata.
     *
     * @param uri
     *          dl uri
     * @return dl metadata.
     * @throws IOException
     */
    public DLMetadata resolve(URI uri) throws IOException;
}
