package com.twitter.distributedlog.service;


import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.callback.LogSegmentListener;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
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

public class MonitorService implements NamespaceListener {

    static final Logger logger = LoggerFactory.getLogger(MonitorService.class);

    private DistributedLogNamespace dlNamespace = null;
    private MonitorServiceClient dlClient = null;
    private DLZkServerSet[] zkServerSets = null;
    private final ScheduledExecutorService executorService =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Map<String, StreamChecker> knownStreams = new HashMap<String, StreamChecker>();

    // Settings
    private int regionId = DistributedLogConstants.LOCAL_REGION_ID;
    private int interval = 100;
    private String streamRegex = null;
    private boolean watchNamespaceChanges = false;
    private boolean handshakeWithClientInfo = false;
    private int heartbeatEveryChecks = 0;
    private int instanceId = -1;
    private int totalInstances = -1;

    // Options
    private final Optional<String> uriArg;
    private final Optional<String> confFileArg;
    private final Optional<String> serverSetArg;
    private final Optional<Integer> intervalArg;
    private final Optional<Integer> regionIdArg;
    private final Optional<String> streamRegexArg;
    private final Optional<Integer> instanceIdArg;
    private final Optional<Integer> totalInstancesArg;
    private final Optional<Integer> heartbeatEveryChecksArg;
    private final Optional<Boolean> handshakeWithClientInfoArg;
    private final Optional<Boolean> watchNamespaceChangesArg;

    // Stats
    private final StatsProvider statsProvider;
    private final StatsReceiver statsReceiver;
    private final StatsReceiver monitorReceiver;
    private final Stat successStat;
    private final Stat failureStat;
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
        public void onSegmentsUpdated(List<LogSegmentMetadata> segments) {
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

    MonitorService(Optional<String> uriArg,
                   Optional<String> confFileArg,
                   Optional<String> serverSetArg,
                   Optional<Integer> intervalArg,
                   Optional<Integer> regionIdArg,
                   Optional<String> streamRegexArg,
                   Optional<Integer> instanceIdArg,
                   Optional<Integer> totalInstancesArg,
                   Optional<Integer> heartbeatEveryChecksArg,
                   Optional<Boolean> handshakeWithClientInfoArg,
                   Optional<Boolean> watchNamespaceChangesArg,
                   StatsReceiver statsReceiver,
                   StatsProvider statsProvider) {
        // options
        this.uriArg = uriArg;
        this.confFileArg = confFileArg;
        this.serverSetArg = serverSetArg;
        this.intervalArg = intervalArg;
        this.regionIdArg = regionIdArg;
        this.streamRegexArg = streamRegexArg;
        this.instanceIdArg = instanceIdArg;
        this.totalInstancesArg = totalInstancesArg;
        this.heartbeatEveryChecksArg = heartbeatEveryChecksArg;
        this.handshakeWithClientInfoArg = handshakeWithClientInfoArg;
        this.watchNamespaceChangesArg = watchNamespaceChangesArg;

        // Stats
        this.statsReceiver = statsReceiver;
        this.monitorReceiver = statsReceiver.scope("monitor");
        this.successStat = monitorReceiver.stat0("success");
        this.failureStat = monitorReceiver.stat0("failure");
        this.statsProvider = statsProvider;
    }

    public void runServer() throws IllegalArgumentException, IOException {
        Preconditions.checkArgument(uriArg.isPresent(),
                "No distributedlog uri provided.");
        Preconditions.checkArgument(serverSetArg.isPresent(),
                "No proxy server set provided.");
        if (intervalArg.isPresent()) {
            interval = intervalArg.get();
        }
        if (regionIdArg.isPresent()) {
            regionId = regionIdArg.get();
        }
        if (streamRegexArg.isPresent()) {
            streamRegex = streamRegexArg.get();
        }
        if (instanceIdArg.isPresent()) {
            instanceId = instanceIdArg.get();
        }
        if (totalInstancesArg.isPresent()) {
            totalInstances = totalInstancesArg.get();
        }
        if (heartbeatEveryChecksArg.isPresent()) {
            heartbeatEveryChecks = heartbeatEveryChecksArg.get();
        }
        if (instanceId < 0 || totalInstances <= 0 || instanceId >= totalInstances) {
            throw new IllegalArgumentException("Invalid instance id or total instances number.");
        }
        handshakeWithClientInfo = handshakeWithClientInfoArg.isPresent();
        watchNamespaceChanges = watchNamespaceChangesArg.isPresent();
        URI uri = URI.create(uriArg.get());
        DistributedLogConfiguration dlConf = new DistributedLogConfiguration();
        if (confFileArg.isPresent()) {
            String configFile = confFileArg.get();
            try {
                dlConf.loadConf(new File(configFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new IOException("Failed to load distributedlog configuration from " + configFile + ".");
            } catch (MalformedURLException e) {
                throw new IOException("Failed to load distributedlog configuration from malformed "
                        + configFile + ".");
            }
        }
        logger.info("Starting stats provider : {}.", statsProvider.getClass());
        statsProvider.start(dlConf);
        String[] serverSetPaths = StringUtils.split(serverSetArg.get(), ",");
        if (serverSetPaths.length == 0) {
            throw new IllegalArgumentException("Invalid serverset paths provided : " + serverSetArg.get());
        }

        ServerSet[] serverSets = createServerSets(serverSetPaths);
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
                .buildMonitorClient();
        runMonitor(dlConf, uri);
    }

    ServerSet[] createServerSets(String[] serverSetPaths) {
        ServerSet[] serverSets = new ServerSet[serverSetPaths.length];
        zkServerSets = new DLZkServerSet[serverSetPaths.length];
        for (int i = 0; i < serverSetPaths.length; i++) {
            String serverSetPath = serverSetPaths[i];
            zkServerSets[i] = parseServerSet(serverSetPath);
            serverSets[i] = zkServerSets[i].getServerSet();
        }
        return serverSets;
    }

    protected DLZkServerSet parseServerSet(String serverSetPath) {
        return DLZkServerSet.of(URI.create(serverSetPath), 60000);
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
        if (null != zkServerSets) {
            for (DLZkServerSet zkServerSet : zkServerSets) {
                zkServerSet.close();
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

}
