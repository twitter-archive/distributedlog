package com.twitter.distributedlog.io;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.function.VoidFunctions;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Utility methods for working with {@link Abortable} objects.
 *
 * @since 0.3.32
 */
public final class Abortables {

    static final Logger logger = LoggerFactory.getLogger(Abortables.class);

    private Abortables() {}

    /**
     * Aborts a {@link Abortable}, with control over whether an {@link IOException} may be thrown.
     * This is primarily useful in a finally block, where a thrown exception needs to be logged but
     * not propagated (otherwise the original exception will be lost).
     *
     * <p>If {@code swallowIOException} is true then we never throw {@code IOException} but merely log it.
     *
     * <p>Example: <pre>   {@code
     *
     *   public void abortStreamNicely() throws IOException {
     *      SomeStream stream = new SomeStream("foo");
     *      try {
     *          // ... code which does something with the stream ...
     *      } catch (IOException ioe) {
     *          // If an exception occurs, we might abort the stream.
     *          Abortables.abort(stream, true);
     *      }
     *   }}</pre>
     *
     * @param abortable the {@code Abortable} object to be aborted, or null, in which case this method
     *                  does nothing.
     * @param swallowIOException if true, don't propagate IO exceptions thrown by the {@code abort} methods
     * @throws IOException if {@code swallowIOException} is false and {@code abort} throws an {@code IOException}
     */
    public static void abort(@Nullable Abortable abortable,
                             boolean swallowIOException)
        throws IOException {
        if (null == abortable) {
            return;
        }
        try {
            abortable.abort();
        } catch (IOException ioe) {
            if (swallowIOException) {
                logger.warn("IOException thrown while aborting Abortable {} : ", abortable, ioe);
            } else {
                throw ioe;
            }
        }
    }

    /**
     * Abort async <i>abortable</i>
     *
     * @param abortable the {@code AsyncAbortable} object to be aborted, or null, in which case this method
     *                  does nothing.
     * @param swallowIOException if true, don't propagate IO exceptions thrown by the {@code abort} methods
     * @throws IOException if {@code swallowIOException} is false and {@code abort} throws an {@code IOException}
     * @see #abort(Abortable, boolean)
     */
    public static void abort(@Nullable AsyncAbortable abortable,
                             boolean swallowIOException)
            throws IOException {
        if (null == abortable) {
            return;
        }
        try {
            FutureUtils.result(abortable.asyncAbort());
        } catch (IOException ioe) {
            if (swallowIOException) {
                logger.warn("IOException thrown while aborting Abortable {} : ", abortable, ioe);
            } else {
                throw ioe;
            }
        }
    }

    /**
     * Aborts the given {@code abortable}, logging any {@code IOException} that's thrown rather than
     * propagating it.
     *
     * While it's not safe in the general case to ignore exceptions that are thrown when aborting an
     * I/O resource, it should generally be safe in the case of a resource that's being used only for
     * reading.
     *
     * @param abortable the {@code Abortable} to be closed, or {@code null} in which case this method
     *                  does nothing.
     */
    public static void abortQuietly(@Nullable Abortable abortable) {
        try {
            abort(abortable, true);
        } catch (IOException e) {
            logger.error("Unexpected IOException thrown while aborting Abortable {} quietly : ", abortable, e);
        }
    }

    /**
     * Aborts the given {@code abortable}, logging any {@code IOException} that's thrown rather than
     * propagating it.
     *
     * While it's not safe in the general case to ignore exceptions that are thrown when aborting an
     * I/O resource, it should generally be safe in the case of a resource that's being used only for
     * reading.
     *
     * @param abortable the {@code AsyncAbortable} to be closed, or {@code null} in which case this method
     *                  does nothing.
     */
    public static void abortQuietly(@Nullable AsyncAbortable abortable) {
        try {
            abort(abortable, true);
        } catch (IOException e) {
            logger.error("Unexpected IOException thrown while aborting Abortable {} quietly : ", abortable, e);
        }
    }

    /**
     * Abort the abortables in sequence.
     *
     * @param executorService
     *          executor service to execute
     * @param abortables
     *          abortables to abort
     * @return future represents the abort future
     */
    public static Future<Void> abortSequence(ExecutorService executorService,
                                             AsyncAbortable... abortables) {
        List<AsyncAbortable> abortableList = Lists.newArrayListWithExpectedSize(abortables.length);
        for (AsyncAbortable abortable : abortables) {
            if (null == abortable) {
                abortableList.add(AsyncAbortable.NULL);
            } else {
                abortableList.add(abortable);
            }
        }
        return FutureUtils.processList(
                abortableList,
                AsyncAbortable.ABORT_FUNC,
                executorService).map(VoidFunctions.LIST_TO_VOID_FUNC);
    }
}
