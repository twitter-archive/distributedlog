package com.twitter.distributedlog.metadata;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.util.Future;

import java.net.URI;
import java.util.Iterator;

/**
 * Interface for log metadata store.
 */
@Beta
public interface LogMetadataStore {

    /**
     * Create a stream and return it is namespace location.
     *
     * @param logName
     *          name of the log
     * @return namespace location that stores this stream.
     */
    Future<URI> createLog(String logName);

    /**
     * Get the location of the log.
     *
     * @param logName
     *          name of the log
     * @return namespace location that stores this stream.
     */
    Future<Optional<URI>> getLogLocation(String logName);

    /**
     * Retrieves logs from the namespace.
     *
     * @return iterator of logs of the namespace.
     */
    Future<Iterator<String>> getLogs();

    /**
     * Register a namespace listener on streams changes.
     *
     * @param listener
     *          namespace listener
     */
    void registerNamespaceListener(NamespaceListener listener);
}
