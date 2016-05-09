/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.util;

import com.twitter.distributedlog.BKTransmitException;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.LockingException;
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
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utilities to process future
 */
public class FutureUtils {

    private static final Logger logger = LoggerFactory.getLogger(FutureUtils.class);

    public static class OrderedFutureEventListener<R>
            implements FutureEventListener<R> {

        public static <R> OrderedFutureEventListener<R> of(
                FutureEventListener<R> listener,
                OrderedScheduler scheduler,
                Object key) {
            return new OrderedFutureEventListener<R>(scheduler, key, listener);
        }

        private final OrderedScheduler scheduler;
        private final Object key;
        private final FutureEventListener<R> listener;

        private OrderedFutureEventListener(OrderedScheduler scheduler,
                                           Object key,
                                           FutureEventListener<R> listener) {
            this.scheduler = scheduler;
            this.key = key;
            this.listener = listener;
        }

        @Override
        public void onSuccess(final R value) {
            scheduler.submit(key, new Runnable() {
                @Override
                public void run() {
                    listener.onSuccess(value);
                }
            });
        }

        @Override
        public void onFailure(final Throwable cause) {
            scheduler.submit(key, new Runnable() {
                @Override
                public void run() {
                    listener.onFailure(cause);
                }
            });
        }
    }

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
            extends Function<Throwable, BoxedUnit>
            implements FutureEventListener<R>, Runnable {

        private volatile boolean interrupted = false;
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
            this.promise.setInterruptHandler(this);
            this.results = new ArrayList<R>();
            this.callbackExecutor = callbackExecutor;
        }

        @Override
        public BoxedUnit apply(Throwable cause) {
            interrupted = true;
            return BoxedUnit.UNIT;
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
            if (interrupted) {
                logger.debug("ListFutureProcessor is interrupted.");
                return;
            }
            if (!itemsIter.hasNext()) {
                promise.setValue(results);
                return;
            }
            processFunc.apply(itemsIter.next()).addEventListener(this);
        }
    }

    /**
     * Process the list of items one by one using the process function <i>processFunc</i>.
     * The process will be stopped immediately if it fails on processing any one.
     *
     * @param collection list of items
     * @param processFunc process function
     * @param callbackExecutor executor to process the item
     * @return future presents the list of processed results
     */
    public static <T, R> Future<List<R>> processList(List<T> collection,
                                                     Function<T, Future<R>> processFunc,
                                                     @Nullable ExecutorService callbackExecutor) {
        ListFutureProcessor<T, R> processor =
                new ListFutureProcessor<T, R>(collection, processFunc, callbackExecutor);
        if (null != callbackExecutor) {
            callbackExecutor.submit(processor);
        } else {
            processor.run();
        }
        return processor.promise;
    }

    /**
     * Await for the result of the future and thrown bk related exceptions.
     *
     * @param result future to wait for
     * @return the result of future
     * @throws BKException when exceptions are thrown by the future. If there is unkown exceptions
     *         thrown from the future, the exceptions will be wrapped into
     *         {@link org.apache.bookkeeper.client.BKException.BKUnexpectedConditionException}.
     */
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

    /**
     * Return the bk exception return code for a <i>throwable</i>.
     *
     * @param throwable the cause of the exception
     * @return the bk exception return code. if the exception isn't bk exceptions,
     *         it would return {@link BKException.Code.UnexpectedConditionException}.
     */
    public static int bkResultCode(Throwable throwable) {
        if (throwable instanceof BKException) {
            return ((BKException)throwable).getCode();
        }
        return BKException.Code.UnexpectedConditionException;
    }

    /**
     * Wait for the result until it completes.
     *
     * @param result result to wait
     * @return the result
     * @throws IOException when encountered exceptions on the result
     */
    public static <T> T result(Future<T> result) throws IOException {
        return result(result, Duration.Top());
    }

    /**
     * Wait for the result for a given <i>duration</i>.
     * <p>If the result is not ready within `duration`, an IOException will thrown wrapping with
     * corresponding {@link com.twitter.util.TimeoutException}.
     *
     * @param result result to wait
     * @param duration duration to wait
     * @return the result
     * @throws IOException when encountered exceptions on the result or waiting for the result.
     */
    public static <T> T result(Future<T> result, Duration duration)
            throws IOException {
        try {
            return Await.result(result, duration);
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

    /**
     * Wait for the result of a lock operation.
     *
     * @param result result to wait
     * @param lockPath path of the lock
     * @return the result
     * @throws LockingException when encountered exceptions on the result of lock operation
     */
    public static <T> T lockResult(Future<T> result, String lockPath) throws LockingException {
        try {
            return Await.result(result);
        } catch (LockingException le) {
            throw le;
        } catch (Exception e) {
            throw new LockingException(lockPath, "Encountered exception on locking ", e);
        }
    }

    /**
     * Convert the <i>throwable</i> to zookeeper related exceptions.
     *
     * @param throwable cause
     * @param path zookeeper path
     * @return zookeeper related exceptions
     */
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

    /**
     * Cancel the future. It would interrupt the future.
     *
     * @param future future to cancel
     */
    public static <T> void cancel(Future<T> future) {
        future.raise(new FutureCancelledException());
    }

    /**
     * Raise an exception to the <i>promise</i> within a given <i>timeout</i> period.
     * If the promise has been satisfied before raising, it won't change the state of the promise.
     *
     * @param promise promise to raise exception
     * @param timeout timeout period
     * @param unit timeout period unit
     * @param cause cause to raise
     * @param scheduler scheduler to execute raising exception
     * @param key the submit key used by the scheduler
     * @return the promise applied with the raise logic
     */
    public static <T> Promise<T> within(final Promise<T> promise,
                                        final long timeout,
                                        final TimeUnit unit,
                                        final Throwable cause,
                                        final OrderedScheduler scheduler,
                                        final Object key) {
        if (timeout < DistributedLogConstants.FUTURE_TIMEOUT_IMMEDIATE || promise.isDefined()) {
            return promise;
        }
        scheduler.schedule(key, new Runnable() {
            @Override
            public void run() {
                logger.info("Raise exception", cause);
                // satisfy the promise
                FutureUtils.setException(promise, cause);
            }
        }, timeout, unit);
        return promise;
    }

    /**
     * Satisfy the <i>promise</i> with provide value in an ordered scheduler.
     * <p>If the promise was already satisfied, nothing will be changed.
     *
     * @param promise promise to satisfy
     * @param value value to satisfy
     * @param scheduler scheduler to satisfy the promise with provided value
     * @param key the submit key of the ordered scheduler
     */
    public static <T> void setValue(final Promise<T> promise,
                                    final T value,
                                    OrderedScheduler scheduler,
                                    Object key) {
        scheduler.submit(key, new Runnable() {
            @Override
            public void run() {
                setValue(promise, value);
            }
        });
    }

    /**
     * Satisfy the <i>promise</i> with provide value.
     * <p>If the promise was already satisfied, nothing will be changed.
     *
     * @param promise promise to satisfy
     * @param value value to satisfy
     * @return true if successfully satisfy the future. false if the promise has been satisfied.
     */
    public static <T> boolean setValue(Promise<T> promise, T value) {
        boolean success = promise.updateIfEmpty(new Return<T>(value));
        if (!success) {
            logger.info("Result set multiple times. Value = '{}', New = 'Return({})'",
                    promise.poll(), value);
        }
        return success;
    }

    /**
     * Satisfy the <i>promise</i> with provided <i>cause</i> in an ordered scheduler.
     *
     * @param promise promise to satisfy
     * @param cause cause to satisfy
     * @param scheduler the scheduler to satisfy the promise
     * @param key submit key of the ordered scheduler
     */
    public static <T> void setException(final Promise<T> promise,
                                        final Throwable throwable,
                                        OrderedScheduler scheduler,
                                        Object key) {
        scheduler.submit(key, new Runnable() {
            @Override
            public void run() {
                setException(promise, throwable);
            }
        });
    }

    /**
     * Satisfy the <i>promise</i> with provided <i>cause</i>.
     *
     * @param promise promise to satisfy
     * @param cause cause to satisfy
     * @return true if successfully satisfy the future. false if the promise has been satisfied.
     */
    public static <T> boolean setException(Promise<T> promise, Throwable cause) {
        boolean success = promise.updateIfEmpty(new Throw<T>(cause));
        if (!success) {
            logger.info("Result set multiple times. Value = '{}', New = 'Throw({})'",
                    promise.poll(), cause);
        }
        return success;
    }

    /**
     * Ignore exception from the <i>future</i>.
     *
     * @param future the original future
     * @return a transformed future ignores exceptions
     */
    public static <T> Promise<Void> ignore(Future<T> future) {
        return ignore(future, null);
    }

    /**
     * Ignore exception from the <i>future</i> and log <i>errorMsg</i> on exceptions
     *
     * @param future the original future
     * @param errorMsg the error message to log on exceptions
     * @return a transformed future ignores exceptions
     */
    public static <T> Promise<Void> ignore(Future<T> future, final String errorMsg) {
        final Promise<Void> promise = new Promise<Void>();
        future.addEventListener(new FutureEventListener<T>() {
            @Override
            public void onSuccess(T value) {
                setValue(promise, null);
            }

            @Override
            public void onFailure(Throwable cause) {
                if (null != errorMsg) {
                    logger.error(errorMsg, cause);
                }
                setValue(promise, null);
            }
        });
        return promise;
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
