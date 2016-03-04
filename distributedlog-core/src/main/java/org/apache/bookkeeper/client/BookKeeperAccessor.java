package org.apache.bookkeeper.client;

import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;

/**
 * Accessor to protected methods in bookkeeper
 */
public class BookKeeperAccessor {

    public static LedgerManager getLedgerManager(BookKeeper bk) {
        return bk.getLedgerManager();
    }

    public static void forceRecoverLedger(LedgerHandle lh,
                                          BookkeeperInternalCallbacks.GenericCallback<Void> cb) {
        lh.recover(cb, null, true);
    }

    public static LedgerMetadata getLedgerMetadata(LedgerHandle lh) {
        return lh.getLedgerMetadata();
    }
}
