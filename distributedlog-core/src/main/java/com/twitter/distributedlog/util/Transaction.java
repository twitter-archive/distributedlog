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

import com.google.common.annotations.Beta;
import com.twitter.util.Future;

/**
 * Util class represents a transaction
 */
@Beta
public interface Transaction<OpResult> {

    /**
     * An operation executed in a transaction.
     */
    interface Op<OpResult> {

        /**
         * Execute after the transaction succeeds
         */
        void commit(OpResult r);

        /**
         * Execute after the transaction fails
         */
        void abort(Throwable t, OpResult r);

    }

    /**
     * Listener on the result of an {@link com.twitter.distributedlog.util.Transaction.Op}.
     *
     * @param <OpResult>
     */
    interface OpListener<OpResult> {

        /**
         * Trigger on operation committed.
         *
         * @param r
         *          result to return
         */
        void onCommit(OpResult r);

        /**
         * Trigger on operation aborted.
         *
         * @param t
         *          reason to abort
         */
        void onAbort(Throwable t);
    }

    /**
     * Add the operation to current transaction.
     *
     * @param operation
     *          operation to execute under current transaction
     */
    void addOp(Op<OpResult> operation);

    /**
     * Execute the current transaction. If the transaction succeed, all operations will be
     * committed (via {@link com.twitter.distributedlog.util.Transaction.Op#commit(Object)}.
     * Otherwise, all operations will be aborted (via {@link Op#abort(Throwable, Object)}).
     *
     * @return future representing the result of transaction execution.
     */
    Future<Void> execute();

    /**
     * Abort current transaction. If this is called and the transaction haven't been executed by
     * {@link #execute()}, it would abort all operations. If the transaction has been executed,
     * the behavior is left up to implementation - if transaction is cancellable, the {@link #abort(Throwable)}
     * could attempt to cancel it.
     *
     * @param reason reason to abort the transaction
     */
    void abort(Throwable reason);

}
