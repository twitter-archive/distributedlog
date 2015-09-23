package com.twitter.distributedlog.bk;

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
                                    boolean ownAllocator) throws IOException {
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
    public void delete() throws IOException {
        throw new UnsupportedOperationException("Can't delete an allocator by delegator");
    }

    @Override
    public void allocate() throws IOException {
        this.allocator.allocate();
    }

    @Override
    public LedgerHandle tryObtain(Object txn) throws IOException {
        return this.allocator.tryObtain(txn);
    }

    @Override
    public void confirmObtain(LedgerHandle lh, Object txn) {
        this.allocator.confirmObtain(lh, txn);
    }

    @Override
    public void abortObtain(LedgerHandle lh) {
        this.allocator.abortObtain(lh);
    }

    @Override
    public void close(boolean cleanup) {
        if (ownAllocator) {
            this.allocator.close(cleanup);
        }
    }
}
