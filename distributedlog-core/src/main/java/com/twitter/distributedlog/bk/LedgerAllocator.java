package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.util.Allocator;
import org.apache.bookkeeper.client.LedgerHandle;

import java.io.IOException;

public interface LedgerAllocator extends Allocator<LedgerHandle, Object> {

    /**
     * Start the ledger allocator. The implementaion should not be blocking call.
     */
    void start() throws IOException;

}
