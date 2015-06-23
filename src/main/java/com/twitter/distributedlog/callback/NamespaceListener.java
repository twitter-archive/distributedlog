package com.twitter.distributedlog.callback;

import com.google.common.annotations.Beta;

import java.util.Iterator;

@Beta
public interface NamespaceListener {

    /**
     * Updated with latest streams.
     *
     * @param streams
     *          latest list of streams under a given namespace.
     */
    void onStreamsChanged(Iterator<String> streams);
}
