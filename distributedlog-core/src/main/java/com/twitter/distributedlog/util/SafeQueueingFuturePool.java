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

import com.google.common.base.Preconditions;

import com.twitter.util.Function0;
import com.twitter.util.FuturePool;
import com.twitter.util.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import scala.runtime.BoxedUnit;

/**
 * Acts like a future pool, but collects failed apply calls into a queue to be applied
 * in-order on close. This happens either in the close thread or after close is called,
 * in the last operation to complete execution.
 * Ops submitted after close will not be scheduled, so its important to ensure no more
 * ops will be applied once close has been called.
 */
public class SafeQueueingFuturePool<T> {

    static final Logger LOG = LoggerFactory.getLogger(SafeQueueingFuturePool.class);

    private boolean closed;
    private int outstanding;
    private ConcurrentLinkedQueue<Function0<T>> queue;
    private FuturePool orderedFuturePool;

    public SafeQueueingFuturePool(FuturePool orderedFuturePool) {
        this.closed = false;
        this.outstanding = 0;
        this.queue = new ConcurrentLinkedQueue<Function0<T>>();
        this.orderedFuturePool = orderedFuturePool;
    }

    public synchronized Future<T> apply(final Function0<T> fn) {
        Preconditions.checkNotNull(fn);
        if (closed) {
            return Future.exception(new RejectedExecutionException("Operation submitted to closed SafeQueueingFuturePool"));
        }
        ++outstanding;
        queue.add(fn);
        Future<T> result = orderedFuturePool.apply(new Function0<T>() {
            @Override
            public T apply() {
                return queue.poll().apply();
            }
            @Override
            public String toString() {
                return fn.toString();
            }
        }).ensure(new Function0<BoxedUnit>() {
            public BoxedUnit apply() {
                if (decrOutstandingAndCheckDone()) {
                    applyAll();
                }
                return null;
            }
        });
        return result;
    }

    private synchronized boolean decrOutstandingAndCheckDone() {
        return --outstanding == 0 && closed;
    }

    public void close() {
        final boolean done;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            done = (outstanding == 0);
        }
        if (done) {
            applyAll();
        }
    }

    private void applyAll() {
        if (!queue.isEmpty()) {
            LOG.info("Applying {} items", queue.size());
        }
        while (!queue.isEmpty()) {
            queue.poll().apply();
        }
    }

    public synchronized int size() {
        return queue.size();
    }
}
