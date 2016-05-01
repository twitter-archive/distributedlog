package com.twitter.distributedlog.io;

import com.twitter.util.Function;
import com.twitter.util.Future;

/**
 * An {@code Abortable} is a source or destination of data that can be aborted.
 * The abort method is invoked to release resources that the object is holding
 * (such as open files). The abort happens when the object is in an error state,
 * which it couldn't be closed gracefully.
 *
 * @see AsyncCloseable
 * @see Abortable
 * @since 0.3.43
 */
public interface AsyncAbortable {

    Function<AsyncAbortable, Future<Void>> ABORT_FUNC = new Function<AsyncAbortable, Future<Void>>() {
        @Override
        public Future<Void> apply(AsyncAbortable abortable) {
            return abortable.asyncAbort();
        }
    };

    AsyncAbortable NULL = new AsyncAbortable() {
        @Override
        public Future<Void> asyncAbort() {
            return Future.Void();
        }
    };

    /**
     * Aborts the object and releases any resources associated with it.
     * If the object is already aborted then invoking this method has no
     * effect.
     *
     * @return future represents the abort result
     */
    Future<Void> asyncAbort();
}
