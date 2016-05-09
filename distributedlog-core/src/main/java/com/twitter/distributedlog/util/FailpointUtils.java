/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.util;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailpointUtils {
    static final Logger logger = LoggerFactory.getLogger(FailpointUtils.class);

    public enum FailPointName {
        FP_StartLogSegmentBeforeLedgerCreate,
        FP_StartLogSegmentAfterLedgerCreate,
        FP_StartLogSegmentAfterInProgressCreate,
        FP_StartLogSegmentOnAssignLogSegmentSequenceNumber,
        FP_FinalizeLedgerBeforeDelete,
        FP_TransmitBeforeAddEntry,
        FP_TransmitComplete,
        FP_WriteInternalLostLock,
        FP_TransmitFailGetBuffer,
        FP_LockUnlockCleanup,
        FP_LockTryCloseRaceCondition,
        FP_LockTryAcquire,
        FP_ZooKeeperConnectionLoss,
        FP_RecoverIncompleteLogSegments,
        FP_LogWriterIssuePending,
    }

    public static interface FailPointAction {
        boolean checkFailPoint() throws IOException;
        boolean checkFailPointNoThrow();
    }

    public static abstract class AbstractFailPointAction implements FailPointAction {
        @Override
        public boolean checkFailPointNoThrow() {
            try {
                return checkFailPoint();
            } catch (IOException ex) {
                logger.error("failpoint action raised unexpected exception");
                return true;
            }
        }
    }

    public static final FailPointAction DEFAULT_ACTION = new AbstractFailPointAction() {
        @Override
        public boolean checkFailPoint() throws IOException {
            return true;
        }
    };

    public static final FailPointAction THROW_ACTION = new AbstractFailPointAction() {
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

    public static boolean checkFailPointNoThrow(FailPointName failPoint) {
        FailPointAction action = failPointState.get(failPoint);

        if (action == null) {
            return false;
        }

        return action.checkFailPointNoThrow();
    }
}
