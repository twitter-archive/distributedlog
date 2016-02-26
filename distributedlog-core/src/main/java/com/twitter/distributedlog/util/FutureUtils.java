package com.twitter.distributedlog.util;

import com.twitter.distributedlog.BKTransmitException;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureCancelledException;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import org.apache.bookkeeper.client.BKException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Utilities to process future
 */
public class FutureUtils {

    private static final Logger logger = LoggerFactory.getLogger(FutureUtils.class);

    public static class FutureEventListenerRunnable<R>
            implements FutureEventListener<R> {

        public static <R> FutureEventListenerRunnable<R> of(
                FutureEventListener<R> listener,
                ExecutorService executorService) {
            return new FutureEventListenerRunnable<R>(executorService, listener);
        }

        private final ExecutorService executorService;
        private final FutureEventListener<R> listener;

        private FutureEventListenerRunnable(ExecutorService executorService,
                                            FutureEventListener<R> listener) {
            this.executorService = executorService;
            this.listener = listener;
        }

        @Override
        public void onSuccess(final R value) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    listener.onSuccess(value);
                }
            });
        }

        @Override
        public void onFailure(final Throwable cause) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    listener.onFailure(cause);
                }
            });
        }
    }

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

    public static <T> T bkResult(Future<T> result) throws BKException {
        try {
            return Await.result(result);
        } catch (BKException bke) {
            throw bke;
        } catch (InterruptedException ie) {
            throw BKException.create(BKException.Code.InterruptedException);
        } catch (Exception e) {
            logger.warn("Encountered unexpected exception on waiting bookkeeper results : ", e);
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }
    }

    public static int bkResultCode(Throwable throwable) {
        if (throwable instanceof BKException) {
            return ((BKException)throwable).getCode();
        }
        return BKException.Code.UnexpectedConditionException;
    }

    public static <T> T result(Future<T> result) throws IOException {
        return result(result, Duration.Top());
    }

    public static <T> T result(Future<T> result, Duration duration)
            throws IOException {
        try {
            return Await.result(result);
        } catch (KeeperException ke) {
            throw new ZKException("Encountered zookeeper exception on waiting result", ke);
        } catch (BKException bke) {
            throw new BKTransmitException("Encountered bookkeeper exception on waiting result", bke.getCode());
        } catch (IOException ioe) {
            throw ioe;
        } catch (InterruptedException ie) {
            throw new DLInterruptedException("Interrupted on waiting result", ie);
        } catch (Exception e) {
            throw new IOException("Encountered exception on waiting result", e);
        }
    }

    public static Throwable zkException(Throwable throwable, String path) {
        if (throwable instanceof KeeperException) {
            return throwable;
        } else if (throwable instanceof ZooKeeperClient.ZooKeeperConnectionException) {
            return KeeperException.create(KeeperException.Code.CONNECTIONLOSS, path);
        } else if (throwable instanceof InterruptedException) {
            return new DLInterruptedException("Interrupted on operating " + path, throwable);
        } else {
            return new UnexpectedException("Encountered unexpected exception on operatiing " + path, throwable);
        }
    }

    public static <T> void cancel(Future<T> future) {
        future.raise(new FutureCancelledException());
    }

    public static <T> boolean setValue(Promise<T> promise, T value) {
        boolean success = promise.updateIfEmpty(new Return<T>(value));
        if (!success) {
            logger.info("Result set multiple times. Value = '{}', New = 'Return({})'",
                    promise.poll(), value);
        }
        return success;
    }

    public static <T> boolean setException(Promise<T> promise, Throwable cause) {
        boolean success = promise.updateIfEmpty(new Throw<T>(cause));
        if (!success) {
            logger.info("Result set multiple times. Value = '{}', New = 'Throw({})'",
                    promise.poll(), cause);
        }
        return success;
    }

    /**
     * Create transmit exception from transmit result.
     *
     * @param transmitResult
     *          transmit result (basically bk exception code)
     * @return transmit exception
     */
    public static BKTransmitException transmitException(int transmitResult) {
        return new BKTransmitException("Failed to write to bookkeeper; Error is ("
            + transmitResult + ") "
            + BKException.getMessage(transmitResult), transmitResult);
    }

}
