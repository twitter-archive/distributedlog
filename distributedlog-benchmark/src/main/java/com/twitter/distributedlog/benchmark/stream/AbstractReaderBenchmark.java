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
package com.twitter.distributedlog.benchmark.stream;

import com.twitter.distributedlog.DistributedLogConstants;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractReaderBenchmark extends StreamBenchmark {

    protected static final Logger logger = LoggerFactory.getLogger(SyncReaderBenchmark.class);

    protected ReadMode readMode = ReadMode.LATEST;
    protected long fromTxId = DistributedLogConstants.INVALID_TXID;
    protected long rewindMs = 0L;
    protected int batchSize = 1;

    protected AbstractReaderBenchmark() {
        options.addOption("t", "tx-id", true, "Transaction ID to start read from when reading in mode 'position'");
        options.addOption("r", "rewind", true, "Time to rewind back to read from when reading in mode 'rewind' (in milliseconds)");
        options.addOption("m", "mode", true, "Read Mode : [oldest, latest, rewind, position]");
        options.addOption("b", "batch-size", true, "Read batch size");
    }

    @Override
    protected void parseCommandLine(CommandLine cmdline) {
        if (cmdline.hasOption("m")) {
            String mode = cmdline.getOptionValue("m");
            try {
                readMode = ReadMode.valueOf(mode.toUpperCase());
            } catch (IllegalArgumentException iae) {
                logger.error("Invalid read mode {}.", mode);
                printUsage();
                System.exit(0);
            }
        } else {
            printUsage();
            System.exit(0);
        }
        if (cmdline.hasOption("t")) {
            fromTxId = Long.parseLong(cmdline.getOptionValue("t"));
        }
        if (cmdline.hasOption("r")) {
            rewindMs = Long.parseLong(cmdline.getOptionValue("r"));
        }
        if (cmdline.hasOption("b")) {
            batchSize = Integer.parseInt(cmdline.getOptionValue("b"));
        }
        logger.info("Start reading from transaction id {}, rewind {} ms.", fromTxId, rewindMs);
    }
}
