package org.apache.bookkeeper.client;

import org.apache.bookkeeper.meta.LedgerManager;

/**
 * Accessor to protected methods in bookkeeper
 */
public class BookKeeperAccessor {

    public static LedgerManager getLedgerManager(BookKeeper bk) {
        return bk.getLedgerManager();
    }
}
