package com.twitter.distributedlog.util;

import com.twitter.distributedlog.io.AsyncCloseable;
import com.twitter.distributedlog.io.AsyncDeleteable;
import com.twitter.distributedlog.util.Transaction.OpListener;
import com.twitter.util.Future;

import java.io.IOException;

/**
 * A common interface to allocate <i>I</i> under transaction <i>T</i>.
 *
 * <h3>Usage Example</h3>
 *
 * Here is an example on demonstrating how `Allocator` works.
 *
 * <pre> {@code
 * Allocator<I, T, R> allocator = ...;
 *
 * // issue an allocate request
 * try {
 *   allocator.allocate();
 * } catch (IOException ioe) {
 *   // handle the exception
 *   ...
 *   return;
 * }
 *
 * // Start a transaction
 * final Transaction<T> txn = ...;
 *
 * // Try obtain object I
 * Future<I> tryObtainFuture = allocator.tryObtain(txn, new OpListener<I>() {
 *     public void onCommit(I resource) {
 *         // the obtain succeed, process with the resource
 *     }
 *     public void onAbort() {
 *         // the obtain failed.
 *     }
 * }).addFutureEventListener(new FutureEventListener() {
 *     public void onSuccess(I resource) {
 *         // the try obtain succeed. but the obtain has not been confirmed or aborted.
 *         // execute the transaction to confirm if it could complete obtain
 *         txn.execute();
 *     }
 *     public void onFailure(Throwable t) {
 *         // handle the failure of try obtain
 *     }
 * });
 *
 * }</pre>
 */
public interface Allocator<I, T> extends AsyncCloseable, AsyncDeleteable {

    /**
     * Issue allocation request to allocate <i>I</i>.
     * The implementation should be non-blocking call.
     *
     * @throws IOException
     *          if fail to request allocating a <i>I</i>.
     */
    void allocate() throws IOException;

    /**
     * Try obtaining an <i>I</i> in a given transaction <i>T</i>. The object obtained is tentative.
     * Whether the object is obtained or aborted is determined by the result of the execution. You could
     * register a listener under this `tryObtain` operation to know whether the object is obtained or
     * aborted.
     *
     * <p>
     * It is a typical two-phases operation on obtaining a resource from allocator.
     * The future returned by this method acts as a `prepare` operation, the resource is tentative obtained
     * from the allocator. The execution of the txn acts as a `commit` operation, the resource is confirmed
     * to be obtained by this transaction. <code>listener</code> is for the whole completion of the obtain.
     * <p>
     * <code>listener</code> is only triggered after `prepare` succeed. if `prepare` failed, no actions will
     * happen to the listener.
     *
     * @param txn
     *          transaction.
     * @return future result returning <i>I</i> that would be obtained under transaction <code>txn</code>.
     */
    Future<I> tryObtain(Transaction<T> txn, OpListener<I> listener);

}
