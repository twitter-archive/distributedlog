package com.twitter.distributedlog.io;

import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Function;
import com.twitter.util.Future;

/**
 * A {@code AsyncCloseable} is a source or destination of data that can be closed asynchronously.
 * The close method is invoked to release resources that the object is
 * holding (such as open files).
 */
public interface AsyncCloseable {

    Function<AsyncCloseable, Future<Void>> CLOSE_FUNC = new Function<AsyncCloseable, Future<Void>>() {
        @Override
        public Future<Void> apply(AsyncCloseable closeable) {
            return closeable.asyncClose();
        }
    };

    Function<AsyncCloseable, Future<Void>> CLOSE_FUNC_IGNORE_ERRORS = new Function<AsyncCloseable, Future<Void>>() {
        @Override
        public Future<Void> apply(AsyncCloseable closeable) {
            return FutureUtils.ignore(closeable.asyncClose());
        }
    };

    AsyncCloseable NULL = new AsyncCloseable() {
        @Override
        public Future<Void> asyncClose() {
            return Future.Void();
        }
    };

    /**
     * Closes this source and releases any system resources associated
     * with it. If the source is already closed then invoking this
     * method has no effect.
     *
     * @return future representing the close result.
     */
    Future<Void> asyncClose();
}
