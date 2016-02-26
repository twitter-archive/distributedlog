package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.util.Transaction;
import com.twitter.distributedlog.util.Transaction.OpListener;
import com.twitter.util.Future;
import org.apache.bookkeeper.client.LedgerHandle;

import java.io.IOException;

/**
 * Delegator of the underlying allocator. If it owns the allocator, it takes
 * the responsibility of start the allocator and close the allocator.
 */
public class LedgerAllocatorDelegator implements LedgerAllocator {

    private final LedgerAllocator allocator;
    private final boolean ownAllocator;

    /**
     * Create an allocator's delegator.
     *
     * @param allocator
     *          the underlying allocator
     * @param ownAllocator
     *          whether to own the allocator
     */
    public LedgerAllocatorDelegator(LedgerAllocator allocator,
                                    boolean ownAllocator)
            throws IOException {
        this.allocator = allocator;
        this.ownAllocator = ownAllocator;
        if (this.ownAllocator) {
            this.allocator.start();
        }
    }

    @Override
    public void start() throws IOException {
        // no-op
    }

    @Override
    public Future<Void> delete() {
        return Future.exception(new UnsupportedOperationException("Can't delete an allocator by delegator"));
    }

    @Override
    public void allocate() throws IOException {
        this.allocator.allocate();
    }

    @Override
    public Future<LedgerHandle> tryObtain(Transaction<Object> txn,
                                          OpListener<LedgerHandle> listener) {
        return this.allocator.tryObtain(txn, listener);
    }

    @Override
    public Future<Void> close() {
        if (ownAllocator) {
            return this.allocator.close();
        } else {
            return Future.value(null);
        }
    }
}
