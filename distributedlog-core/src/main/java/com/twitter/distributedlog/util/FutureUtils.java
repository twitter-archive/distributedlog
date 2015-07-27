package com.twitter.distributedlog.util;

import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureCancelledException;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Utilities to process future
 */
public class FutureUtils {

    private static class ListFutureProcessor<T, R>
            implements FutureEventListener<R>, Runnable {

        private final Iterator<T> itemsIter;
        private final Function<T, Future<R>> processFunc;
        private final Promise<List<R>> promise;
        private final List<R> results;
        private final ExecutorService callbackExecutor;

        ListFutureProcessor(List<T> items,
                            Function<T, Future<R>> processFunc,
                            ExecutorService callbackExecutor) {
            this.itemsIter = items.iterator();
            this.processFunc = processFunc;
            this.promise = new Promise<List<R>>();
            this.results = new ArrayList<R>();
            this.callbackExecutor = callbackExecutor;
        }

        @Override
        public void onSuccess(R value) {
            results.add(value);
            if (null == callbackExecutor) {
                run();
            } else {
                callbackExecutor.submit(this);
            }
        }

        @Override
        public void onFailure(final Throwable cause) {
            if (null == callbackExecutor) {
                promise.setException(cause);
            } else {
                callbackExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        promise.setException(cause);
                    }
                });
            }
        }

        @Override
        public void run() {
            if (!itemsIter.hasNext()) {
                promise.setValue(results);
                return;
            }
            processFunc.apply(itemsIter.next()).addEventListener(this);
        }
    }

    public static <T, R> Future<List<R>> processList(List<T> collection,
                                                     Function<T, Future<R>> processFunc,
                                                     ExecutorService callbackExecutor) {
        ListFutureProcessor<T, R> processor =
                new ListFutureProcessor<T, R>(collection, processFunc, callbackExecutor);
        if (null != callbackExecutor) {
            callbackExecutor.submit(processor);
        } else {
            processor.run();
        }
        return processor.promise;
    }

    public static <T> T result(Future<T> result) throws IOException {
        try {
            return Await.result(result);
        } catch (IOException ioe) {
            throw ioe;
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on waiting result", ie);
        } catch (Exception e) {
            throw new IOException("Encountered exception on waiting result", e);
        }
    }

    public static <T> void cancel(Future<T> future) {
        future.raise(new FutureCancelledException());
    }

}
