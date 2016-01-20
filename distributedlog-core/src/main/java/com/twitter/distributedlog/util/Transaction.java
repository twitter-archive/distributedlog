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
        void abort(OpResult r);

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
     * Otherwise, all operations will be aborted (via {@link com.twitter.distributedlog.util.Transaction.Op#abort(Object)}.
     *
     * @return future representing the result of transaction execution.
     */
    Future<Void> execute();

}
