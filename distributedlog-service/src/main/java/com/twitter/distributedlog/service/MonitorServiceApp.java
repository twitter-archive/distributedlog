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
package com.twitter.distributedlog.service;

import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.twitter.distributedlog.util.CommandLineUtils.*;

public class MonitorServiceApp {

    static final Logger logger = LoggerFactory.getLogger(MonitorServiceApp.class);

    final static String USAGE = "MonitorService [-u <uri>] [-c <conf>] [-s serverset]";
    final String[] args;
    final Options options = new Options();

    private MonitorServiceApp(String[] args) {
        this.args = args;
        // prepare options
        options.addOption("u", "uri", true, "DistributedLog URI");
        options.addOption("c", "conf", true, "DistributedLog Configuration File");
        options.addOption("s", "serverset", true, "Proxy Server Set");
        options.addOption("i", "interval", true, "Check interval");
        options.addOption("d", "region", true, "Region ID");
        options.addOption("p", "provider", true, "DistributedLog Stats Provider");
        options.addOption("f", "filter", true, "Filter streams by regex");
        options.addOption("w", "watch", false, "Watch stream changes under a given namespace");
        options.addOption("n", "instance_id", true, "Instance ID");
        options.addOption("t", "total_instances", true, "Total instances");
        options.addOption("hck", "heartbeat-num-checks", true, "Send a heartbeat after num checks");
        options.addOption("hsci", "handshake-with-client-info", false, "Enable handshaking with client info");
    }

    void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(USAGE, options);
    }

    private void run() {
        try {
            logger.info("Running monitor service.");
            BasicParser parser = new BasicParser();
            CommandLine cmdline = parser.parse(options, args);
            runCmd(cmdline);
        } catch (ParseException pe) {
            printUsage();
            Runtime.getRuntime().exit(-1);
        } catch (IOException ie) {
            logger.error("Failed to start monitor service : ", ie);
            Runtime.getRuntime().exit(-1);
        }
    }

    void runCmd(CommandLine cmdline) throws IOException {
        StatsProvider statsProvider = new NullStatsProvider();
        if (cmdline.hasOption("p")) {
            String providerClass = cmdline.getOptionValue("p");
            statsProvider = ReflectionUtils.newInstance(providerClass, StatsProvider.class);
        }
        StatsReceiver statsReceiver = NullStatsReceiver.get();

        final MonitorService monitorService = new MonitorService(
                getOptionalStringArg(cmdline, "u"),
                getOptionalStringArg(cmdline, "c"),
                getOptionalStringArg(cmdline, "s"),
                getOptionalIntegerArg(cmdline, "i"),
                getOptionalIntegerArg(cmdline, "d"),
                getOptionalStringArg(cmdline, "f"),
                getOptionalIntegerArg(cmdline, "n"),
                getOptionalIntegerArg(cmdline, "t"),
                getOptionalIntegerArg(cmdline, "hck"),
                getOptionalBooleanArg(cmdline, "hsci"),
                getOptionalBooleanArg(cmdline, "w"),
                statsReceiver,
                statsProvider);

        monitorService.runServer();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Closing monitor service.");
                monitorService.close();
                logger.info("Closed monitor service.");
            }
        });
        try {
            monitorService.join();
        } catch (InterruptedException ie) {
            logger.warn("Interrupted when waiting monitor service to be finished : ", ie);
        }
    }

    public static void main(String[] args) {
        final MonitorServiceApp launcher = new MonitorServiceApp(args);
        launcher.run();
    }
}
