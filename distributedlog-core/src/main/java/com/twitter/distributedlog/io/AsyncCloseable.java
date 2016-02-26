package com.twitter.distributedlog.io;

import com.twitter.util.Future;

/**
 * A {@code AsyncCloseable} is a source or destination of data that can be closed asynchronously.
 * The close method is invoked to release resources that the object is
 * holding (such as open files).
 */
public interface AsyncCloseable {
    /**
     * Closes this source and releases any system resources associated
     * with it. If the source is already closed then invoking this
     * method has no effect.
     *
     * @return future representing the close result.
     */
    Future<Void> close();
}
