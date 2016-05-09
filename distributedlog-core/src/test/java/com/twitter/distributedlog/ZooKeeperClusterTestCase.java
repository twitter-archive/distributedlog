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

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

public class ZooKeeperClusterTestCase {

    protected static File zkDir;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;
    protected static int zkPort;

    @BeforeClass
    public static void setupZooKeeper() throws Exception {
        zkDir = IOUtils.createTempDir("zookeeper", ZooKeeperClusterTestCase.class.getName());
        Pair<ZooKeeperServerShim, Integer> serverAndPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkDir);
        zks = serverAndPort.getLeft();
        zkPort = serverAndPort.getRight();
        zkServers = "127.0.0.1:" + zkPort;
    }

    @AfterClass
    public static void shutdownZooKeeper() throws Exception {
        zks.stop();
        if (null != zkDir) {
            FileUtils.deleteDirectory(zkDir);
        }
    }
}
