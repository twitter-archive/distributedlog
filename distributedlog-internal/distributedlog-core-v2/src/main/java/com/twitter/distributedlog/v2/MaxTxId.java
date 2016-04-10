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
package com.twitter.distributedlog.v2;

import java.io.IOException;

import com.twitter.distributedlog.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for storing and reading
 * the max seen txid in zookeeper
 */
class MaxTxId {
    static final Logger LOG = LoggerFactory.getLogger(MaxTxId.class);

    private final ZooKeeperClient zkc;
    private final String path;

    private Stat currentStat;

    MaxTxId(ZooKeeperClient zkc, String path) {
        this.zkc = zkc;
        this.path = path;
    }

    /**
     * Store the highest TxID encountered so far so that we
     * can enforce the monotonically non-decreasing property
     * This is best effort as this enforcement is only done
     *
     * @param maxTxId - the maximum transaction id seen so far
     * @throws IOException
     */
    synchronized void store(long maxTxId) throws IOException {
        long currentMax = get();
        if (currentMax < maxTxId) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Setting maxTxId to " + maxTxId);
            }
            String txidStr = Long.toString(maxTxId);
            try {
                if (currentStat != null) {
                    currentStat = zkc.get().setData(path, txidStr.getBytes("UTF-8"),
                        currentStat.getVersion());
                } else {
                    zkc.get().create(path, txidStr.getBytes("UTF-8"),
                        zkc.getDefaultACL(), CreateMode.PERSISTENT);
                }
            } catch (Exception e) {
                LOG.error("Error writing new MaxTxId value {}", maxTxId, e);
            }
        }
    }

    synchronized long get() throws IOException {
        try {
            currentStat = zkc.get().exists(path, false);
            if (currentStat == null) {
                return 0;
            } else {
                byte[] bytes = zkc.get().getData(path, false, currentStat);
                String txidString = new String(bytes, "UTF-8");
                return Long.valueOf(txidString);
            }
        } catch (Exception e) {
            LOG.error("Error reading the max tx id from zk for path {}", path, e);
        }

        return 0;
    }
}
