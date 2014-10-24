package com.twitter.distributedlog;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class FailpointUtils {
    public enum FailPointName {
        FP_StartLogSegmentBeforeLedgerCreate,
        FP_StartLogSegmentAfterLedgerCreate,
        FP_StartLogSegmentAfterInProgressCreate,
        FP_FinalizeLedgerBeforeDelete,
        FP_TransmitBeforeAddEntry,
        FP_TransmitComplete,
        FP_WriteInternalLostLock,
        FP_ZooKeeperConnectionLoss
    }

    static interface FailPointAction {
        boolean checkFailPoint() throws IOException;
    }

    static final FailPointAction DEFAULT_ACTION = new FailPointAction() {
        @Override
        public boolean checkFailPoint() throws IOException {
            return true;
        }
    };

    static final FailPointAction THROW_ACTION = new FailPointAction() {
        @Override
        public boolean checkFailPoint() throws IOException {
            throw new IOException("Throw ioexception for failure point");
        }
    };

    public enum FailPointActions {
        FailPointAction_Default,
        FailPointAction_Throw
    }

    static ConcurrentHashMap<FailPointName, FailPointAction> failPointState =
            new ConcurrentHashMap<FailPointName, FailPointAction>();

    public static void setFailpoint(FailPointName failpoint, FailPointActions action) {
        FailPointAction fpAction = null;
        switch (action) {
        case FailPointAction_Default:
            fpAction = DEFAULT_ACTION;
            break;
        case FailPointAction_Throw:
            fpAction = THROW_ACTION;
            break;
        default:
            break;
        }
        setFailpoint(failpoint, fpAction);
    }

    public static void setFailpoint(FailPointName failpoint, FailPointAction action) {
        if (null != action) {
            failPointState.put(failpoint, action);
        }
    }

    public static void removeFailpoint(FailPointName failpoint) {
        failPointState.remove(failpoint);
    }

    public static boolean checkFailPoint(FailPointName failPoint) throws IOException {
        FailPointAction action = failPointState.get(failPoint);

        if (action == null) {
            return false;
        }

        try {
            return action.checkFailPoint();
        } catch (IOException ioe) {
            throw new IOException("Induced Exception at:" + failPoint, ioe);
        }
    }
}
