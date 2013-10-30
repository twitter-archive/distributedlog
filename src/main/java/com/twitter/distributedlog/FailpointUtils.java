package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

class FailpointUtils {
    enum FailPointName {
        FP_StartLogSegmentAfterLedgerCreate,
        FP_StartLogSegmentAfterInProgressCreate,
        FP_FinalizeLedgerBeforeDelete
    }

    enum FailPointActions {
        FailPointAction_Default,
        FailPointAction_Throw
    }

    static ConcurrentHashMap<FailPointName, FailPointActions> failPointState = new ConcurrentHashMap<FailPointName, FailPointActions>();

    public static void setFailpoint(FailPointName failpoint, FailPointActions action) {
        failPointState.put(failpoint, action);
    }

    public static void removeFailpoint(FailPointName failpoint) {
        failPointState.remove(failpoint);
    }

    public static boolean checkFailPoint(FailPointName failPoint) throws IOException {
        FailPointActions action = failPointState.get(failPoint);

        if (action == null) {
            return false;
        } else if (action == FailPointActions.FailPointAction_Throw) {
            throw new IOException("Induced Exception at:" + failPoint);
        }

        return true;
    }
}
