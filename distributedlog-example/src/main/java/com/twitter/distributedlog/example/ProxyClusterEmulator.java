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
package com.twitter.distributedlog.example;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.service.DistributedLogCluster;
import com.twitter.distributedlog.service.DistributedLogServerApp;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for DistributedLogCluster emulator
 */
public class ProxyClusterEmulator {
    static final Logger LOG = LoggerFactory.getLogger(ProxyClusterEmulator.class);

    private final DistributedLogCluster dlCluster;
    private final String[] args;

    public ProxyClusterEmulator(String[] args) throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setImmediateFlushEnabled(true);
        conf.setOutputBufferSize(0);
        conf.setPeriodicFlushFrequencyMilliSeconds(0);
        conf.setLockTimeout(0);
        this.dlCluster = DistributedLogCluster.newBuilder()
            .numBookies(3)
            .shouldStartZK(true)
            .zkServers("127.0.0.1")
            .shouldStartProxy(false) // We'll start it separately so we can pass args.
            .dlConf(conf)
            .build();
        this.args = args;
    }

    public void start() throws Exception {
        dlCluster.start();

        // Run the server with bl cluster info.
        String[] extendedArgs = new String[args.length + 2];
        System.arraycopy(args, 0, extendedArgs, 0, args.length);
        extendedArgs[extendedArgs.length - 2] = "-u";
        extendedArgs[extendedArgs.length - 1] = dlCluster.getUri().toString();
        LOG.debug("Using args {}", Arrays.toString(extendedArgs));
        DistributedLogServerApp.main(extendedArgs);
    }

    public void stop() throws Exception {
        dlCluster.stop();
    }

    public static void main(String[] args) throws Exception {
        ProxyClusterEmulator emulator = null;
        try {
            emulator = new ProxyClusterEmulator(args);
            emulator.start();
        } catch (Exception ex) {
            if (null != emulator) {
                emulator.stop();
            }
            System.out.println("Exception occurred running emulator " + ex);
        }
    }
}
