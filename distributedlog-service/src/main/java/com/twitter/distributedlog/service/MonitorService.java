package com.twitter.distributedlog.service;


import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.Iterator;
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
    private String streamRegex = null;
    private DistributedLogNamespace dlNamespace = null;
    private ZooKeeperClient[] zkClients = null;
    private DistributedLogClientBuilder.DistributedLogClientImpl dlClient = null;
    private StatsProvider statsProvider = null;
    private boolean watchNamespaceChanges = false;
    private boolean handshakeWithClientInfo = false;
    private int heartbeatEveryChecks = 0;
    private int instanceId = -1;
    private int totalInstances = -1;
    private final ScheduledExecutorService executorService =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Map<String, StreamChecker> knownStreams = new HashMap<String, StreamChecker>();
    private final StatsReceiver statsReceiver = new OstrichStatsReceiver();
    // Stats
    private final StatsReceiver monitorReceiver = statsReceiver.scope("monitor");
    private final Stat successStat = monitorReceiver.stat0("success");
    private final Stat failureStat = monitorReceiver.stat0("failure");
    // Hash Function
    private final HashFunction hashFunction = Hashing.md5();

    class StreamChecker implements Runnable, FutureEventListener<Void>, LogSegmentListener {
        private final String name;
        private volatile boolean closed = false;
        private volatile boolean checking = false;
        private final Stopwatch stopwatch = Stopwatch.createUnstarted();
        private DistributedLogManager dlm = null;
        private int numChecks = 0;

        StreamChecker(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            if (null == dlm) {
                try {
                    dlm = dlNamespace.openLog(name);
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
                stopwatch.reset().start();
                boolean sendHeartBeat;
                if (heartbeatEveryChecks > 0) {
                    synchronized (this) {
                        ++numChecks;
                        if (numChecks >= Integer.MAX_VALUE) {
                            numChecks = 0;
                        }
                        sendHeartBeat = (numChecks % heartbeatEveryChecks) == 0;
                    }
                } else {
                    sendHeartBeat = false;
                }
                if (sendHeartBeat) {
                    dlClient.heartbeat(name).addEventListener(this);
                } else {
                    dlClient.check(name).addEventListener(this);
                }
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
        if (cmdline.hasOption("f")) {
            streamRegex = cmdline.getOptionValue("f");
        }
        if (cmdline.hasOption("n")) {
            instanceId = Integer.parseInt(cmdline.getOptionValue("n"));
        }
        if (cmdline.hasOption("t")) {
            totalInstances = Integer.parseInt(cmdline.getOptionValue("t"));
        }
        if (cmdline.hasOption("hck")) {
            heartbeatEveryChecks = Integer.parseInt(cmdline.getOptionValue("hck"));
        }
        handshakeWithClientInfo = cmdline.hasOption("hsci");
        if (instanceId < 0 || totalInstances <= 0 || instanceId >= totalInstances) {
            throw new ParseException("Invalid instance id or total instances number.");
        }
        watchNamespaceChanges = cmdline.hasOption('w');
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
        statsProvider = new NullStatsProvider();
        if (cmdline.hasOption("p")) {
            String providerClass = cmdline.getOptionValue("p");
            statsProvider = ReflectionUtils.newInstance(providerClass, StatsProvider.class);
        }
        logger.info("Starting stats provider : {}.", statsProvider.getClass());
        statsProvider.start(dlConf);
        String[] serverSetPaths = StringUtils.split(cmdline.getOptionValue("s"), ",");
        if (serverSetPaths.length == 0) {
            throw new ParseException("Invalid serverset paths provided : " + cmdline.getOptionValue("s"));
        }

        ServerSet[] serverSets = new ServerSet[serverSetPaths.length];
        zkClients = new ZooKeeperClient[serverSetPaths.length];
        for (int i = 0; i < serverSetPaths.length; i++) {
            String serverSetPath = serverSetPaths[i];
            Pair<ZooKeeperClient, ServerSet> ssPair = Utils.parseServerSet(serverSetPath);
            zkClients[i] = ssPair.getLeft();
            serverSets[i] = ssPair.getRight();
        }

        ServerSet local = serverSets[0];
        ServerSet[] remotes  = new ServerSet[serverSets.length - 1];
        System.arraycopy(serverSets, 1, remotes, 0, remotes.length);

        dlClient = DistributedLogClientBuilder.newBuilder()
                .name("monitor")
                .clientId(ClientId$.MODULE$.apply("monitor"))
                .redirectBackoffMaxMs(50)
                .redirectBackoffStartMs(100)
                .requestTimeoutMs(2000)
                .maxRedirects(2)
                .serverSets(local, remotes)
                .streamNameRegex(streamRegex)
                .handshakeWithClientInfo(handshakeWithClientInfo)
                .clientBuilder(ClientBuilder.get()
                        .connectTimeout(Duration.fromSeconds(1))
                        .tcpConnectTimeout(Duration.fromSeconds(1))
                        .requestTimeout(Duration.fromSeconds(2))
                        .hostConnectionLimit(2)
                        .hostConnectionCoresize(2)
                        .keepAlive(true)
                        .failFast(false))
                .statsReceiver(monitorReceiver.scope("client"))
                .buildClient();
        runMonitor(dlConf, uri);
    }

    @Override
    public void onStreamsChanged(Iterator<String> streams) {
        Set<String> newSet = new HashSet<String>();
        while (streams.hasNext()) {
            String s = streams.next();
            if (null == streamRegex || s.matches(streamRegex)) {
                if (Math.abs(hashFunction.hashUnencodedChars(s).asInt()) % totalInstances == instanceId) {
                    newSet.add(s);
                }
            }
        }
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
        // stats
        statsProvider.getStatsLogger("monitor").registerGauge("num_streams", new org.apache.bookkeeper.stats.Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return knownStreams.size();
            }
        });
        logger.info("Construct dl namespace @ {}", dlUri);
        dlNamespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(dlUri)
                .build();
        if (watchNamespaceChanges) {
            dlNamespace.registerNamespaceListener(this);
        } else {
            onStreamsChanged(dlNamespace.getLogs());
        }
    }

    /**
     * Close the server
     */
    public void close() {
        logger.info("Closing monitor service.");
        if (null != dlClient) {
            dlClient.close();
        }
        if (null != zkClients) {
            for (ZooKeeperClient zkClient : zkClients) {
                zkClient.close();
            }
        }
        if (null != dlNamespace) {
            dlNamespace.close();
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted on waiting shutting down monitor executor service : ", e);
        }
        if (null != statsProvider) {
            statsProvider.stop();
        }
        keepAliveLatch.countDown();
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
