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
package com.twitter.distributedlog.kafka;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class DLFutureRecordMetadata implements Future<RecordMetadata> {

    private final String topic;
    private final com.twitter.util.Future<DLSN> dlsnFuture;
    private final Callback callback;

    DLFutureRecordMetadata(final String topic,
                           com.twitter.util.Future<DLSN> dlsnFuture,
                           final Callback callback) {
        this.topic = topic;
        this.dlsnFuture = dlsnFuture;
        this.callback = callback;

        this.dlsnFuture.addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                callback.onCompletion(null, new IOException(cause));
            }

            @Override
            public void onSuccess(DLSN value) {
                callback.onCompletion(new RecordMetadata(new TopicPartition(topic, 0), -1L, -1L), null);
            }
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        dlsnFuture.cancel();
        // it is hard to figure out if it is successful or not.
        // so return false here.
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return dlsnFuture.isDefined();
    }

    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        try {
            DLSN dlsn = FutureUtils.result(dlsnFuture);
            // TODO: align the DLSN concepts with kafka concepts
            return new RecordMetadata(new TopicPartition(topic, 0), -1L, -1L);
        } catch (DLInterruptedException e) {
            throw new InterruptedException("Interrupted on waiting for response");
        } catch (IOException e) {
            throw new ExecutionException("Error on waiting for response", e);
        }
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            DLSN dlsn = FutureUtils.result(dlsnFuture, Duration.apply(timeout, unit));
            // TODO: align the DLSN concepts with kafka concepts
            return new RecordMetadata(new TopicPartition(topic, 0), -1L, -1L);
        } catch (DLInterruptedException e) {
            throw new InterruptedException("Interrupted on waiting for response");
        } catch (IOException e) {
            throw new ExecutionException("Error on waiting for response", e);
        }
    }
}
