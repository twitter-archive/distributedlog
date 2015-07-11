package com.twitter.distributedlog.io;

import java.io.IOException;

/**
 * An {@code Abortable} is a source or destination of data that can be aborted.
 * The abort method is invoked to release resources that the object is holding
 * (such as open files). The abort happens when the object is in an error state,
 * which it couldn't be closed gracefully.
 *
 * @see {@link java.io.Closeable}
 * @since 0.3.32
 */
public interface Abortable {

    /**
     * Aborts the object and releases any resources associated with it.
     * If the object is already aborted then invoking this method has no
     * effect.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void abort() throws IOException;
}
