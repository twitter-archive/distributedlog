package com.twitter.distributedlog.callback;

import java.util.Collection;

public interface NamespaceListener {

    /**
     * Updated with latest streams.
     *
     * @param streams
     *          latest list of streams under a given namespace.
     */
    void onStreamsChanged(Collection<String> streams);
}
