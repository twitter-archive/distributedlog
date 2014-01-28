package com.twitter.distributedlog.service;


import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common_internal.zookeeper.TwitterServerSet;
import com.twitter.common_internal.zookeeper.TwitterZk;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MonitorService implements Runnable, NamespaceListener {

    static final Logger logger = LoggerFactory.getLogger(MonitorService.class);

    final static String USAGE = "MonitorService [-u <uri>] [-c <conf>] [-s serverset]";
    final String[] args;
    final Options options = new Options();

    private int regionId = DistributedLogConstants.LOCAL_REGION_ID;
    private int interval = 100;
    private DistributedLogManagerFactory dlFactory = null;
    private ZooKeeperClient zkClient = null;
    private DistributedLogClientBuilder.DistributedLogClientImpl dlClient = null;
    private final ScheduledExecutorService executorService =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Map<String, StreamChecker> knownStreams = new HashMap<String, StreamChecker>();
    private final StatsReceiver statsReceiver = new OstrichStatsReceiver();
    // Stats
    private final StatsReceiver monitorReceiver = statsReceiver.scope("monitor");
    private final Stat successStat = monitorReceiver.stat0("success");
    private final Stat failureStat = monitorReceiver.stat0("failure");

    class StreamChecker implements Runnable, FutureEventListener<Void>, LogSegmentListener {
        private final String name;
        private volatile boolean closed = false;
        private volatile boolean checking = false;
        private final Stopwatch stopwatch = new Stopwatch();
        private DistributedLogManager dlm = null;

        StreamChecker(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            if (null == dlm) {
                try {
                    dlm = dlFactory.createDistributedLogManagerWithSharedClients(name);
                    dlm.registerListener(this);
                } catch (IOException e) {
                    if (null != dlm) {
                        try {
                            dlm.close();
                        } catch (IOException e1) {
                            logger.error("Failed to close dlm for {} : ", name, e1);
                        }
                        dlm = null;
                    }
                    executorService.schedule(this, interval, TimeUnit.MILLISECONDS);
                }
            } else {
                stopwatch.start();
                dlClient.check(name).addEventListener(this);
            }
        }

        @Override
        public void onSegmentsUpdated(List<LogSegmentLedgerMetadata> segments) {
            if (segments.size() > 0 && segments.get(0).getRegionId() == regionId) {
                if (!checking) {
                    logger.info("Start checking stream {}.", name);
                    checking = true;
                    run();
                }
            } else {
                if (checking) {
                    logger.info("Stop checking stream {}.", name);
                }
            }
        }

        @Override
        public void onSuccess(Void value) {
            successStat.add(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            scheduleCheck();
        }

        @Override
        public void onFailure(Throwable cause) {
            failureStat.add(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            scheduleCheck();
        }

        private void scheduleCheck() {
            if (closed) {
                return;
            }
            if (!checking) {
                return;
            }
            try {
                executorService.schedule(this, interval, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                logger.error("Failed to schedule checking stream {} in {} ms : ",
                        new Object[] { name, interval, ree });
            }
        }

        private void close() {
            closed = true;
            if (null != dlm) {
                try {
                    dlm.close();
                } catch (IOException e) {
                    logger.error("Failed to close dlm for {} : ", name, e);
                }
            }
        }
    }

    MonitorService(String[] args) {
        this.args = args;
        // prepare options
        options.addOption("u", "uri", true, "DistributedLog URI");
        options.addOption("c", "conf", true, "DistributedLog Configuration File");
        options.addOption("s", "serverset", true, "Proxy Server Set");
        options.addOption("i", "interval", true, "Check interval");
        options.addOption("d", "region", true, "Region ID");
    }

    void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(USAGE, options);
    }

    @Override
    public void run() {
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

    void runCmd(CommandLine cmdline) throws ParseException, IOException {
        if (!cmdline.hasOption("u")) {
            throw new ParseException("No distributedlog uri provided.");
        }
        if (!cmdline.hasOption("s")) {
            throw new ParseException("No proxy server set provided.");
        }
        if (cmdline.hasOption("i")) {
            interval = Integer.parseInt(cmdline.getOptionValue("i"));
        }
        if (cmdline.hasOption("d")) {
            regionId = Integer.parseInt(cmdline.getOptionValue("d"));
        }
        URI uri = URI.create(cmdline.getOptionValue("u"));
        DistributedLogConfiguration dlConf = new DistributedLogConfiguration();
        if (cmdline.hasOption("c")) {
            String configFile = cmdline.getOptionValue("c");
            try {
                dlConf.loadConf(new File(configFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new ParseException("Failed to load distributedlog configuration from " + configFile + ".");
            } catch (MalformedURLException e) {
                throw new ParseException("Failed to load distributedlog configuration from malformed "
                        + configFile + ".");
            }
        }
        String[] serverSetPath = StringUtils.split(cmdline.getOptionValue("s"), '/');
        if (serverSetPath.length != 3) {
            throw new ParseException("Invalid serverset path : " + cmdline.getOptionValue("s"));
        }
        TwitterServerSet.Service zkService = new TwitterServerSet.Service(serverSetPath[0], serverSetPath[1], serverSetPath[2]);
        zkClient = TwitterServerSet.clientBuilder(zkService).zkEndpoints(TwitterZk.SD_ZK_ENDPOINTS).build();
        ServerSet serverSet = TwitterServerSet.create(zkClient, zkService);
        dlClient = DistributedLogClientBuilder.newBuilder()
                .name("monitor")
                .clientId(ClientId$.MODULE$.apply("monitor"))
                .redirectBackoffMaxMs(50)
                .redirectBackoffStartMs(100)
                .maxRedirects(2)
                .serverSet(serverSet)
                .clientBuilder(ClientBuilder.get()
                        .connectTimeout(Duration.fromSeconds(5))
                        .tcpConnectTimeout(Duration.fromSeconds(2))
                        .requestTimeout(Duration.fromSeconds(5))
                        .hostConnectionLimit(10)
                        .hostConnectionIdleTime(Duration.fromSeconds(1)))
                .statsReceiver(statsReceiver)
                .buildClient();
        runMonitor(dlConf, uri);
    }

    @Override
    public void onStreamsChanged(Collection<String> streams) {
        Set<String> newSet = new HashSet<String>(streams);
        List<StreamChecker> tasksToCancel = new ArrayList<StreamChecker>();
        synchronized (knownStreams) {
            Set<String> knownStreamSet = new HashSet<String>(knownStreams.keySet());
            Set<String> removedStreams = Sets.difference(knownStreamSet, newSet).immutableCopy();
            Set<String> addedStreams = Sets.difference(newSet, knownStreamSet).immutableCopy();
            for (String s : removedStreams) {
                StreamChecker task = knownStreams.remove(s);
                if (null != task) {
                    logger.info("Removed stream {}", s);
                    tasksToCancel.add(task);
                }
            }
            for (String s : addedStreams) {
                if (!knownStreams.containsKey(s)) {
                    logger.info("Added stream {}", s);
                    StreamChecker sc = new StreamChecker(s);
                    knownStreams.put(s, sc);
                    sc.run();
                }
            }
        }
        for (StreamChecker sc : tasksToCancel) {
            sc.close();
        }
    }

    void runMonitor(DistributedLogConfiguration conf, URI dlUri) throws IOException {
        logger.info("Construct dl factory @ {}", dlUri);
        dlFactory = new DistributedLogManagerFactory(conf, dlUri);
        dlFactory.registerNamespaceListener(this);
    }

    /**
     * Close the server
     */
    public void close() {
        logger.info("Closing monitor service.");
        if (null != dlClient) {
            dlClient.close();
        }
        if (null != zkClient) {
            zkClient.close();
        }
        if (null != dlFactory) {
            dlFactory.close();
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted on waiting shutting down monitor executor service : ", e);
        }
        logger.info("Closed monitor service.");
    }

    public void join() throws InterruptedException {
        keepAliveLatch.await();
    }

    public static MonitorService run(String[] args) {
        final MonitorService service = new MonitorService(args);
        service.run();
        return service;
    }

    public static void main(String[] args) {
        final MonitorService service = run(args);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Closing monitor service.");
                service.close();
                logger.info("Closed monitor service.");
            }
        });
        try {
            service.join();
        } catch (InterruptedException ie) {
            logger.warn("Interrupted when waiting monitor service to be finished : ", ie);
        }
    }
}
