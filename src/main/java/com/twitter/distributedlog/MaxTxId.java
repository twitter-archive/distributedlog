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
package com.twitter.distributedlog;

import com.twitter.distributedlog.zk.DataWithStat;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Utility class for storing and reading
 * the max seen txid in zookeeper
 */
class MaxTxId {
    static final Logger LOG = LoggerFactory.getLogger(MaxTxId.class);

    private final ZooKeeperClient zkc;
    private final String path;

    private long currentMax;

    MaxTxId(ZooKeeperClient zkc, String path, DataWithStat dataWithStat) {
        this.zkc = zkc;
        this.path = path;
        try {
            this.currentMax = toTxId(dataWithStat.getData());
        } catch (UnsupportedEncodingException e) {
            LOG.warn("Invalid txn id stored in {} : e", path, e);
            this.currentMax = 0L;
        }
    }

    String getZkPath() {
        return path;
    }

    synchronized void setMaxTxId(long txId) {
        if (this.currentMax < txId) {
            this.currentMax = txId;
        }
    }

    static long toTxId(byte[] data) throws UnsupportedEncodingException {
        String txidString = new String(data, UTF_8);
        return Long.valueOf(txidString);
    }

    static byte[] toBytes(long txId) {
        String txidString = Long.toString(txId);
        return txidString.getBytes(UTF_8);
    }

    synchronized byte[] couldStore(long maxTxId) {
        if (currentMax < maxTxId) {
            return toBytes(maxTxId);
        } else {
            return null;
        }
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
        if (currentMax < maxTxId) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Setting maxTxId to " + maxTxId);
            }
            String txidStr = Long.toString(maxTxId);
            try {
                Stat stat = zkc.get().setData(path, txidStr.getBytes("UTF-8"), -1);
                currentMax = maxTxId;
            } catch (Exception e) {
                LOG.error("Error writing new MaxTxId value {}", maxTxId, e);
            }
        }
    }

    synchronized long get() throws IOException {
        return currentMax;
    }

}
