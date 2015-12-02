package com.twitter.distributedlog.benchmark;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.benchmark.utils.ShiftableRateLimiter;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Benchmarker {

    static final Logger logger = LoggerFactory.getLogger(Benchmarker.class);

    final static String USAGE = "Benchmarker [-u <uri>] [-c <conf>] [-s serverset] [-m (read|write|dlwrite)]";

    final String[] args;
    final Options options = new Options();

    int rate = 100;
    int maxRate = 1000;
    int changeRate = 100;
    int changeRateSeconds = 1800;
    int concurrency = 10;
    String streamPrefix = "dlog-loadtest-";
    int shardId = -1;
    int numStreams = 10;
    List<String> serversetPaths = new ArrayList<String>();
    List<String> finagleNames = new ArrayList<String>();
    int msgSize = 256;
    String mode = null;
    int durationMins = 60;
    URI dlUri = null;
    int batchSize = 0;
    int readersPerStream = 1;
    Integer maxStreamId = null;
    int truncationInterval = 3600;
    Integer startStreamId = null;
    Integer endStreamId = null;
    int hostConnectionCoreSize = 10;
    int hostConnectionLimit = 10;
    boolean thriftmux = false;
    boolean handshakeWithClientInfo = false;
    boolean readFromHead = false;
    int sendBufferSize = 1024 * 1024;
    int recvBufferSize = 1024 * 1024;

    final DistributedLogConfiguration conf = new DistributedLogConfiguration();
    final StatsReceiver statsReceiver = new OstrichStatsReceiver();
    StatsProvider statsProvider = null;

    Benchmarker(String[] args) {
        this.args = args;
        // prepare options
        options.addOption("s", "serverset", true, "Proxy Server Set (separated by ',')");
        options.addOption("fn", "finagle-name", true, "Write proxy finagle name (separated by ',')");
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
        options.addOption("mr", "max-rate", true, "Maximum Rate limit (requests/second)");
        options.addOption("cr", "change-rate", true, "Rate to increase each change period (requests/second)");
        options.addOption("ci", "change-interval", true, "Rate to increase period, seconds");
        options.addOption("t", "concurrency", true, "Concurrency (number of threads)");
        options.addOption("m", "mode", true, "Benchmark mode (read/write)");
        options.addOption("rps", "readers-per-stream", true, "Number readers per stream");
        options.addOption("msid", "max-stream-id", true, "Max Stream ID");
        options.addOption("ti", "truncation-interval", true, "Truncation interval in seconds");
        options.addOption("ssid", "start-stream-id", true, "Start Stream ID");
        options.addOption("esid", "end-stream-id", true, "Start Stream ID");
        options.addOption("hccs", "host-connection-core-size", true, "Finagle hostConnectionCoreSize");
        options.addOption("hcl", "host-connection-limit", true, "Finagle hostConnectionLimit");
        options.addOption("mx", "thriftmux", false, "Enable thriftmux (write mode only)");
        options.addOption("hsci", "handshake-with-client-info", false, "Enable handshaking with client info");
        options.addOption("rfh", "read-from-head", false, "Read from head of the stream");
        options.addOption("sb", "send-buffer", true, "Channel send buffer size, in bytes");
        options.addOption("rb", "recv-buffer", true, "Channel recv buffer size, in bytes");
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
            String serversetPathStr = cmdline.getOptionValue("s");
            serversetPaths = Arrays.asList(StringUtils.split(serversetPathStr, ','));
        }
        if (cmdline.hasOption("fn")) {
            String finagleNameStr = cmdline.getOptionValue("fn");
            finagleNames = Arrays.asList(StringUtils.split(finagleNameStr, ','));
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
        if (cmdline.hasOption("mr")) {
            maxRate = Integer.parseInt(cmdline.getOptionValue("mr"));
        }
        if (cmdline.hasOption("cr")) {
            changeRate = Integer.parseInt(cmdline.getOptionValue("cr"));
        }
        if (cmdline.hasOption("ci")) {
            changeRateSeconds = Integer.parseInt(cmdline.getOptionValue("ci"));
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
        if (cmdline.hasOption("rps")) {
            readersPerStream = Integer.parseInt(cmdline.getOptionValue("rps"));
        }
        if (cmdline.hasOption("msid")) {
            maxStreamId = Integer.parseInt(cmdline.getOptionValue("msid"));
        }
        if (cmdline.hasOption("ti")) {
            truncationInterval = Integer.parseInt(cmdline.getOptionValue("ti"));
        }
        if (cmdline.hasOption("ssid")) {
            startStreamId = Integer.parseInt(cmdline.getOptionValue("ssid"));
        }
        if (cmdline.hasOption("esid")) {
            endStreamId = Integer.parseInt(cmdline.getOptionValue("esid"));
        }
        if (cmdline.hasOption("hccs")) {
            hostConnectionCoreSize = Integer.parseInt(cmdline.getOptionValue("hccs"));
        }
        if (cmdline.hasOption("hcl")) {
            hostConnectionLimit = Integer.parseInt(cmdline.getOptionValue("hcl"));
        }
        if (cmdline.hasOption("sb")) {
            sendBufferSize = Integer.parseInt(cmdline.getOptionValue("sb"));
        }
        if (cmdline.hasOption("rb")) {
            recvBufferSize = Integer.parseInt(cmdline.getOptionValue("rb"));
        }
        thriftmux = cmdline.hasOption("mx");
        handshakeWithClientInfo = cmdline.hasOption("hsci");
        readFromHead = cmdline.hasOption("rfh");

        Preconditions.checkArgument(shardId >= 0, "shardId must be >= 0");
        Preconditions.checkArgument(numStreams > 0, "numStreams must be > 0");
        Preconditions.checkArgument(durationMins > 0, "durationMins must be > 0");
        Preconditions.checkArgument(streamPrefix != null, "streamPrefix must be defined");
        Preconditions.checkArgument(hostConnectionCoreSize > 0, "host connection core size must be > 0");
        Preconditions.checkArgument(hostConnectionLimit > 0, "host connection limit must be > 0");

        if (cmdline.hasOption("p")) {
            statsProvider = ReflectionUtils.newInstance(cmdline.getOptionValue("p"), StatsProvider.class);
        } else {
            statsProvider = new NullStatsProvider();
        }

        logger.info("Starting stats provider : {}.", statsProvider.getClass());
        statsProvider.start(conf);

        Worker w = null;
        if (mode.startsWith("read")) {
            w = runReader();
        } else if (mode.startsWith("write")) {
            w = runWriter();
        } else if (mode.startsWith("dlwrite")) {
            w = runDLWriter();
        } else if (mode.startsWith("dlread")) {
            w = runDLReader();
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

        Runtime.getRuntime().exit(0);
    }

    Worker runWriter() {
        Preconditions.checkArgument(!finagleNames.isEmpty() || !serversetPaths.isEmpty(),
                "either serverset paths or finagle-names required");
        Preconditions.checkArgument(msgSize > 0, "messagesize must be greater than 0");
        Preconditions.checkArgument(rate > 0, "rate must be greater than 0");
        Preconditions.checkArgument(maxRate >= rate, "max rate must be greater than rate");
        Preconditions.checkArgument(changeRate >= 0, "change rate must be positive");
        Preconditions.checkArgument(changeRateSeconds >= 0, "change rate must be positive");
        Preconditions.checkArgument(concurrency > 0, "concurrency must be greater than 0");

        ShiftableRateLimiter rateLimiter =
                new ShiftableRateLimiter(rate, maxRate, changeRate, changeRateSeconds, TimeUnit.SECONDS);

        return new WriterWorker(streamPrefix,
                null == startStreamId ? shardId * numStreams : startStreamId,
                null == endStreamId ? (shardId + 1) * numStreams : endStreamId,
                rateLimiter,
                concurrency,
                msgSize,
                batchSize,
                hostConnectionCoreSize,
                hostConnectionLimit,
                serversetPaths,
                finagleNames,
                statsReceiver.scope("write_client"),
                statsProvider.getStatsLogger("write"),
                thriftmux,
                handshakeWithClientInfo,
                sendBufferSize,
                recvBufferSize);
    }

    Worker runDLWriter() throws IOException {
        Preconditions.checkNotNull(dlUri, "dlUri must be defined");
        Preconditions.checkArgument(rate > 0, "rate must be greater than 0");
        Preconditions.checkArgument(maxRate >= rate, "max rate must be greater than rate");
        Preconditions.checkArgument(changeRate >= 0, "change rate must be positive");
        Preconditions.checkArgument(changeRateSeconds >= 0, "change rate must be positive");
        Preconditions.checkArgument(concurrency > 0, "concurrency must be greater than 0");

        ShiftableRateLimiter rateLimiter =
                new ShiftableRateLimiter(rate, maxRate, changeRate, changeRateSeconds, TimeUnit.SECONDS);

        return new DLWriterWorker(conf,
                dlUri,
                streamPrefix,
                shardId * numStreams,
                (shardId + 1) * numStreams,
                rateLimiter,
                concurrency,
                msgSize,
                statsProvider.getStatsLogger("dlwrite"));
    }

    Worker runReader() throws IOException {
        Preconditions.checkArgument(!finagleNames.isEmpty() || !serversetPaths.isEmpty(),
                "either serverset paths or finagle-names required");
        Preconditions.checkArgument(concurrency > 0, "concurrency must be greater than 0");
        Preconditions.checkArgument(truncationInterval > 0, "truncation interval should be greater than 0");
        return runReaderInternal(serversetPaths, finagleNames, truncationInterval);
    }

    Worker runDLReader() throws IOException {
        return runReaderInternal(new ArrayList<String>(), new ArrayList<String>(), 0);
    }

    private Worker runReaderInternal(List<String> serversetPaths,
                                     List<String> finagleNames,
                                     int truncationInterval) throws IOException {
        Preconditions.checkNotNull(dlUri);

        int ssid = null == startStreamId ? shardId * numStreams : startStreamId;
        int esid = null == endStreamId ? (shardId + readersPerStream) * numStreams : endStreamId;
        if (null != maxStreamId) {
            esid = Math.min(esid, maxStreamId);
        }

        return new ReaderWorker(conf,
                dlUri,
                streamPrefix,
                ssid,
                esid,
                concurrency,
                serversetPaths,
                finagleNames,
                truncationInterval,
                readFromHead,
                statsReceiver,
                statsProvider.getStatsLogger("dlreader"));
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
