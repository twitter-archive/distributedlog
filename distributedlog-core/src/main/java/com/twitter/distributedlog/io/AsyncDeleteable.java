package com.twitter.distributedlog.io;

import com.twitter.util.Future;

/**
 * A {@code AsyncDeleteable} is a source or destination of data that can be deleted asynchronously.
 * This delete method is invoked to delete the source.
 */
public interface AsyncDeleteable {
    /**
     * Releases any system resources associated with this and delete the source. If the source is
     * already deleted then invoking this method has no effect.
     *
     * @return future representing the deletion result.
     */
    Future<Void> delete();
}
