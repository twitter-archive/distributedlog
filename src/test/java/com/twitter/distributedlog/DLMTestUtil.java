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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Utility class for setting up bookkeeper ensembles
 * and bringing individual bookies up and down
 */
class DLMTestUtil {
    protected static final Log LOG = LogFactory.getLog(DLMTestUtil.class);
    private final static String zkEnsemble = "127.0.0.1:2181";

    static URI createDLMURI(String path) throws Exception {
        return URI.create("distributedlog://" + zkEnsemble + path);
    }

    static BKLogPartitionWriteHandler createNewBKDLM(DistributedLogConfiguration conf,
                                             String path) throws Exception {
        return createNewBKDLM(new PartitionId(0), conf, path);
    }

    static DistributedLogManager createNewDLM(DistributedLogConfiguration conf,
                                             String name) throws Exception {
        return DistributedLogManagerFactory.createDistributedLogManager(name, conf, createDLMURI("/" + name));
    }


    static BKLogPartitionWriteHandler createNewBKDLM(PartitionId p,
                                             DistributedLogConfiguration conf, String path) throws Exception{
        return new BKLogPartitionWriteHandler(path, p, conf, createDLMURI("/" + path), null, null);
    }

    static long getNumberofLogRecords(DistributedLogManager bkdlm, PartitionId partition, long startTxId) throws IOException {
        long numLogRecs = 0;
        LogReader reader = bkdlm.getInputStream(partition, startTxId);
        LogRecord record = reader.readNext(false);
        while (null != record) {
            numLogRecs++;
            verifyLogRecord(record);
            record = reader.readNext(false);
        }

        return numLogRecs;
    }


    static LogRecord getLogRecordInstance(long txId) {
        return new LogRecord(txId, (new Long(txId)).toString().getBytes());
    }

    static void verifyLogRecord(LogRecord record) {
        assertEquals((new Long(record.getTransactionId())).toString().getBytes().length, record.getPayload().length);
        assertArrayEquals((new Long(record.getTransactionId())).toString().getBytes(), record.getPayload());
    }
}
