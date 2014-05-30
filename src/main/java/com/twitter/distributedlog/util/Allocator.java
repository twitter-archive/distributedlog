package com.twitter.distributedlog.util;

import java.io.IOException;

/**
 * A common interface to allocate <i>I</i> under transaction <i>T</i>.
 */
public interface Allocator<I, T, R> {

    /**
     * Issue allocation request to allocate <i>I</i>.
     *
     * @throws IOException
     *          if fail to request allocating a <i>I</i>.
     */
    void allocate() throws IOException;

    /**
     * Try obtaining an <i>I</i> in a given transaction <i>T</i>.
     *
     * @param txn
     *          transaction.
     * @return I
     * @throws IOException if failed to obtain allocated <i>I</i>.
     */
    I tryObtain(T txn) throws IOException;

    /**
     * Confirm obtaining allocated <i>I</i> with result <i>R</i>.
     *
     * @param obj
     *          object allocated.
     * @param result
     *          result of transaction <i>T</i> in {@link #tryObtain(Object)}.
     */
    void confirmObtain(I obj, R result);

    /**
     * Abort obtaining <i>obj</i>.
     *
     * @param obj
     *          object allocated.
     */
    void abortObtain(I obj);

    /**
     * Close the allocator.
     */
    void close(boolean cleanup);
}
