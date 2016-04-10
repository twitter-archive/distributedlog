package com.twitter.distributedlog.service;

import com.google.common.base.Optional;
import com.twitter.app.Flag;
import com.twitter.app.Flaggable;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.server.AbstractTwitterServer;
import org.apache.bookkeeper.stats.CachingStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.twitter.finagle.FinagleStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Twitter Server Based Monitor Service
 */
public class TwitterMonitorServer extends AbstractTwitterServer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterMonitorServer.class);

    private static class TwitterMonitorService extends MonitorService {

        TwitterMonitorService(Optional<String> uriArg,
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
            super(uriArg,
                    confFileArg,
                    serverSetArg,
                    intervalArg,
                    regionIdArg,
                    streamRegexArg,
                    instanceIdArg,
                    totalInstancesArg,
                    heartbeatEveryChecksArg,
                    handshakeWithClientInfoArg,
                    watchNamespaceChangesArg,
                    statsReceiver,
                    statsProvider);
        }

        @Override
        protected DLZkServerSet parseServerSet(String serverSetPath) {
            return TwitterServerSetUtils.parseServerSet(serverSetPath);
        }
    }

    private final Flag<String> uriFlag =
            flag().create("u", "", "DistributedLog URI", Flaggable.ofString());
    private final Flag<String> confFlag =
            flag().create("c", "", "DistributedLog Configuration File", Flaggable.ofString());
    private final Flag<String> serverSetFlag =
            flag().create("s", "", "Proxy Server Set", Flaggable.ofString());
    private final Flag<Integer> intervalFlag =
            flag().create("i", 100, "Check interval", Flaggable.ofJavaInteger());
    private final Flag<Integer> regionIdFlag =
            flag().create("d", DistributedLogConstants.LOCAL_REGION_ID, "Region ID", Flaggable.ofJavaInteger());
    private final Flag<String> streamRegexFlag =
            flag().create("f", "*", "Filter streams by regex", Flaggable.ofString());
    private final Flag<Boolean> watchNamespaceChangesFlag =
            flag().create("w", false, "Watch stream changes under a given namespace", Flaggable.ofJavaBoolean());
    private final Flag<Integer> instanceIdFlag =
            flag().create("n", -1, "Instance ID", Flaggable.ofJavaInteger());
    private final Flag<Integer> totalInstancesFlag =
            flag().create("t", -1, "Total instances", Flaggable.ofJavaInteger());
    private final Flag<Integer> heartbeatEveryChecksFlag =
            flag().create("hck", 0, "Send a heartbeat after num checks", Flaggable.ofJavaInteger());
    private final Flag<Boolean> handshakeWithClientInfoFlag =
            flag().create("hsci", false, "Enable handshaking with client info", Flaggable.ofJavaBoolean());

    private <T> Optional<T> getOptionalFlag(Flag<T> flag) {
        if (flag.isDefined()) {
            return Optional.of(flag.apply());
        } else {
            return Optional.absent();
        }
    }

    @Override
    public void main() throws Throwable {
        StatsReceiver statsReceiver = statsReceiver();
        StatsProvider statsProvider =
                new CachingStatsProvider(new FinagleStatsProvider(statsReceiver));

        final MonitorService monitorService = new TwitterMonitorService(
                getOptionalFlag(uriFlag),
                getOptionalFlag(confFlag),
                getOptionalFlag(serverSetFlag),
                getOptionalFlag(intervalFlag),
                getOptionalFlag(regionIdFlag),
                getOptionalFlag(streamRegexFlag),
                getOptionalFlag(instanceIdFlag),
                getOptionalFlag(totalInstancesFlag),
                getOptionalFlag(heartbeatEveryChecksFlag),
                getOptionalFlag(handshakeWithClientInfoFlag),
                getOptionalFlag(watchNamespaceChangesFlag),
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

    /**
     * Wrapper to run DistributedLog Monitor Server
     */
    public static class Main {

        public static void main(String[] args) {
            new DistributedLogTwitterServer().main(args);
        }

    }

}
