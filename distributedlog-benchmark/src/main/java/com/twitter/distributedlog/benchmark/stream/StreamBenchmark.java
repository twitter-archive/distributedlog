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

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

/**
 * Benchmark Streams.
 */
public abstract class StreamBenchmark {

    private static final Logger logger = LoggerFactory.getLogger(StreamBenchmark.class);

    private static final String USAGE = "StreamBenchmark <benchmark-class> [options]";

    protected final Options options = new Options();
    protected URI uri;
    protected DistributedLogConfiguration conf;
    protected StatsProvider statsProvider;
    protected String streamName;

    protected StreamBenchmark() {
        options.addOption("c", "conf", true, "Configuration File");
        options.addOption("u", "uri", true, "DistributedLog URI");
        options.addOption("p", "stats-provider", true, "Stats Provider");
        options.addOption("s", "stream", true, "Stream Name");
        options.addOption("h", "help", false, "Print usage.");
    }

    protected Options getOptions() {
        return options;
    }

    protected void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(USAGE, options);
    }

    protected void parseCommandLine(String[] args)
            throws Exception {
        BasicParser parser = new BasicParser();
        CommandLine cmdline = parser.parse(options, args);
        if (cmdline.hasOption("h")) {
            printUsage();
            System.exit(0);
        }
        if (cmdline.hasOption("u")) {
            this.uri = URI.create(cmdline.getOptionValue("u"));
        } else {
            printUsage();
            System.exit(0);
        }
        this.conf = new DistributedLogConfiguration();
        if (cmdline.hasOption("c")) {
            String configFile = cmdline.getOptionValue("c");
            this.conf.loadConf(new File(configFile).toURI().toURL());
        }
        if (cmdline.hasOption("p")) {
            statsProvider = ReflectionUtils.newInstance(cmdline.getOptionValue("p"), StatsProvider.class);
        } else {
            statsProvider = new NullStatsProvider();
        }
        if (cmdline.hasOption("s")) {
            this.streamName = cmdline.getOptionValue("s");
        } else {
            printUsage();
            System.exit(0);
        }
        parseCommandLine(cmdline);
    }

    protected abstract void parseCommandLine(CommandLine cmdline);

    protected void run(String[] args) throws Exception {
        logger.info("Parsing arguments for benchmark : {}", args);
        // parse command line
        parseCommandLine(args);
        statsProvider.start(conf);
        // run the benchmark
        StatsLogger statsLogger = statsProvider.getStatsLogger("dl");
        DistributedLogNamespace namespace =
                DistributedLogNamespaceBuilder.newBuilder()
                        .conf(conf)
                        .uri(uri)
                        .statsLogger(statsLogger)
                        .build();
        try {
            benchmark(namespace, streamName, statsProvider.getStatsLogger("benchmark"));
        } finally {
            namespace.close();
            statsProvider.stop();
        }
    }

    protected abstract void benchmark(DistributedLogNamespace namespace,
                                      String logName,
                                      StatsLogger statsLogger);

    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.err.println(USAGE);
            return;
        }
        String benchmarkClassName = args[0];
        StreamBenchmark benchmark = ReflectionUtils.newInstance(
                benchmarkClassName, StreamBenchmark.class);
        benchmark.run(args);
    }
}
