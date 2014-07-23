package com.twitter.distributedlog.benchmark;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class Benchmarker {

    static final Logger logger = LoggerFactory.getLogger(Benchmarker.class);

    final static String USAGE = "Benchmarker [-u <uri>] [-c <conf>] [-s serverset] [-m (read|write|dlwrite)]";

    final String[] args;
    final Options options = new Options();

    int rate = 1000;
    int concurrency = 10;
    String streamPrefix = "dlog-loadtest-";
    int shardId = -1;
    int numStreams = 10;
    String serversetPath = null;
    int msgSize = 256;
    String mode = null;
    int durationMins = 60;
    URI dlUri = null;
    int batchSize = 0;

    final DistributedLogConfiguration conf = new DistributedLogConfiguration();
    final StatsReceiver statsReceiver = new OstrichStatsReceiver();
    StatsProvider statsProvider = null;

    Benchmarker(String[] args) {
        this.args = args;
        // prepare options
        options.addOption("s", "serverset", true, "Proxy Server Set");
        options.addOption("c", "conf", true, "DistributedLog Configuration File");
        options.addOption("u", "uri", true, "DistributedLog URI");
        options.addOption("i", "shard", true, "Shard Id");
        options.addOption("p", "provider", true, "DistributedLog Stats Provider");
        options.addOption("d", "duration", true, "Duration (minutes)");
        options.addOption("sp", "streamprefix", true, "Stream Prefix");
        options.addOption("sc", "streamcount", true, "Number of Streams");
        options.addOption("ms", "messagesize", true, "Message Size (bytes)");
        options.addOption("bs", "batchsize", true, "Batch Size");
        options.addOption("r", "rate", true, "Rate limit (requests/second)");
        options.addOption("t", "concurrency", true, "Concurrency (number of threads)");
        options.addOption("m", "mode", true, "Benchmark mode (read/write)");
        options.addOption("h", "help", false, "Print usage.");
    }

    void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(USAGE, options);
    }

    void run() throws Exception {
        logger.info("Running benchmark.");
        BasicParser parser = new BasicParser();
        CommandLine cmdline = parser.parse(options, args);
        if (cmdline.hasOption("h")) {
            printUsage();
            System.exit(0);
        }
        if (cmdline.hasOption("s")) {
            serversetPath = cmdline.getOptionValue("s");
        }
        if (cmdline.hasOption("i")) {
            shardId = Integer.parseInt(cmdline.getOptionValue("i"));
        }
        if (cmdline.hasOption("d")) {
            durationMins = Integer.parseInt(cmdline.getOptionValue("d"));
        }
        if (cmdline.hasOption("sp")) {
            streamPrefix = cmdline.getOptionValue("sp");
        }
        if (cmdline.hasOption("sc")) {
            numStreams = Integer.parseInt(cmdline.getOptionValue("sc"));
        }
        if (cmdline.hasOption("ms")) {
            msgSize = Integer.parseInt(cmdline.getOptionValue("ms"));
        }
        if (cmdline.hasOption("r")) {
            rate = Integer.parseInt(cmdline.getOptionValue("r"));
        }
        if (cmdline.hasOption("t")) {
            concurrency = Integer.parseInt(cmdline.getOptionValue("t"));
        }
        if (cmdline.hasOption("m")) {
            mode = cmdline.getOptionValue("m");
        }
        if (cmdline.hasOption("u")) {
            dlUri = URI.create(cmdline.getOptionValue("u"));
        }
        if (cmdline.hasOption("bs")) {
            batchSize = Integer.parseInt(cmdline.getOptionValue("bs"));
            Preconditions.checkArgument("write" != mode, "batchSize supported only for mode=write");
        }
        if (cmdline.hasOption("c")) {
            String configFile = cmdline.getOptionValue("c");
            conf.loadConf(new File(configFile).toURI().toURL());
        }

        Preconditions.checkArgument(shardId >= 0, "shardId must be >= 0");
        Preconditions.checkArgument(numStreams > 0, "numStreams must be > 0");
        Preconditions.checkArgument(durationMins > 0, "durationMins must be > 0");
        Preconditions.checkArgument(streamPrefix != null, "streamPrefix must be defined");

        if (cmdline.hasOption("p")) {
            statsProvider = ReflectionUtils.newInstance(cmdline.getOptionValue("p"), StatsProvider.class);
        } else {
            statsProvider = new NullStatsProvider();
        }

        logger.info("Starting stats provider : {}.", statsProvider.getClass());
        statsProvider.start(conf);

        Worker w = null;
        if ("read".equals(mode)) {
            w = runReader();
        } else if ("write".equals(mode)) {
            w = runWriter();
        } else if ("dlwrite".equals(mode)) {
            w = runDLWriter();
        }

        if (w == null) {
            throw new IOException("Unknown mode " + mode + " to run the benchmark.");
        }

        Thread workerThread = new Thread(w, mode + "-benchmark-thread");
        workerThread.start();

        TimeUnit.MINUTES.sleep(durationMins);

        logger.info("{} minutes passed, exiting...", durationMins);
        w.close();

        if (null != statsProvider) {
            statsProvider.stop();
        }
    }

    Worker runWriter() {
        Preconditions.checkArgument(serversetPath != null, "serversetPath is required");
        Preconditions.checkArgument(msgSize > 0, "messagesize must be greater than 0");
        Preconditions.checkArgument(rate > 0, "rate must be greater than 0");
        Preconditions.checkArgument(concurrency > 0, "concurrency must be greater than 0");

        return new WriterWorker(streamPrefix,
                shardId * numStreams,
                (shardId + 1) * numStreams,
                rate,
                concurrency,
                msgSize,
                batchSize,
                serversetPath,
                statsReceiver.scope("write_client"),
                statsProvider.getStatsLogger("write")
                );
    }

    Worker runDLWriter() throws IOException {
        Preconditions.checkNotNull(dlUri, "dlUri must be defined");

        return new DLWriterWorker(conf,
                dlUri,
                streamPrefix,
                shardId * numStreams,
                (shardId + 1) * numStreams,
                rate,
                concurrency,
                msgSize,
                statsProvider.getStatsLogger("dlwrite"));
    }

    Worker runReader() {
        // TODO:
        return null;
    }

    public static void main(String[] args) {
        Benchmarker benchmarker = new Benchmarker(args);
        try {
            benchmarker.run();
        } catch (Exception e) {
            logger.info("Benchmark quitted due to : ", e);
        }
    }

}
