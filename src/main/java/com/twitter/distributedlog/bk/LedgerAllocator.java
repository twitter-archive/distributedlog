package com.twitter.distributedlog.bk;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;

import java.io.IOException;

public interface LedgerAllocator {

    /**
     * Issue allocation request to allocate ledger.
     *
     * @throws IOException
     *          if fail to request allocating a ledger.
     */
    void allocate() throws IOException;

    /**
     * Try obtaining a ledger in given <i>txn</i> context.
     *
     * @param txn
     *          transaction.
     * @return ledger handle
     * @throws IOException if failed to obtain allocated ledger handle.
     */
    LedgerHandle tryObtain(Transaction txn) throws IOException;

    /**
     * Confirm obtaining allocated <i>ledger</i> with <i>result</i>.
     *
     * @param ledger
     *          allocated ledger
     * @param result
     *          obtain result
     */
    void confirmObtain(LedgerHandle ledger, OpResult result);

    /**
     * Abort obtaining <i>ledger</i>.
     *
     * @param ledger
     *          allocated ledger
     */
    void abortObtain(LedgerHandle ledger);

    /**
     * Close ledger allocator.
     */
    void close();
}
