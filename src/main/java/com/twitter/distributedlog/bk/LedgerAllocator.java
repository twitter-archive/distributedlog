package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.util.Allocator;
import org.apache.bookkeeper.client.LedgerHandle;

import java.io.IOException;

public interface LedgerAllocator extends Allocator<LedgerHandle, Object, Object> {

    /**
     * Start the ledger allocator.
     */
    void start() throws IOException;

    /**
     * Delete a given allocator.
     *
     * @throws IOException
     */
    void delete() throws IOException;
}
