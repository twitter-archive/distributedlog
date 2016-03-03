package com.twitter.distributedlog.service;

import com.google.common.base.Optional;
import com.twitter.app.Flag;
import com.twitter.app.Flaggable;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.feature.DeciderFeatureProvider;
import com.twitter.distributedlog.service.config.ServerConfiguration;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.server.AbstractTwitterServer;
import org.apache.bookkeeper.stats.CachingStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.twitter.finagle.FinagleStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/**
 * Twitter Server Based DistributedLog Write Proxy
 */
public class DistributedLogTwitterServer extends AbstractTwitterServer {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogTwitterServer.class);

    private static class DistributedLogTwitterServerInternal extends DistributedLogServer {

        DistributedLogTwitterServerInternal(Optional<String> uri,
                                            Optional<String> conf,
                                            Optional<String> stringConf,
                                            Optional<Integer> port,
                                            Optional<Integer> statsPort,
                                            Optional<Integer> shardId,
                                            Optional<String> announcePath,
                                            Optional<Boolean> thriftmux,
                                            StatsReceiver statsReceiver,
                                            StatsProvider statsProvider) {
            super(uri,
                  conf,
                  stringConf,
                  port,
                  statsPort,
                  shardId,
                  announcePath,
                  thriftmux,
                  statsReceiver,
                  statsProvider);
        }

        @Override
        protected void preRun(DistributedLogConfiguration conf, ServerConfiguration serverConf) {
            super.preRun(conf, serverConf);
            conf.setFeatureProviderClass(DeciderFeatureProvider.class);
        }
    }

    private final Flag<String> uriFlag =
            flag().create("u", "", "DistributedLog URI", Flaggable.ofString());
    private final Flag<String> confFlag =
            flag().create("c", "", "DistributedLog Configuration File", Flaggable.ofString());
    private final Flag<String> streamConfFlag =
            flag().create("sc", "", "Per Stream Configuration Directory", Flaggable.ofString());
    private final Flag<Integer> portFlag =
            flag().create("p", 0, "DistributedLog Server Port", Flaggable.ofJavaInteger());
    private final Flag<Boolean> muxFlag =
            flag().create("mx", false, "Is thriftmux enabled", Flaggable.ofJavaBoolean());

    private <T> Optional<T> getOptionalFlag(Flag<T> flag) {
        if (flag.isDefined()) {
            return Optional.of(flag.apply());
        } else {
            return Optional.absent();
        }
    }

    @Override
    public void main() throws Throwable {
        runServer(getOptionalFlag(uriFlag));
    }

    public void runServer(Optional<String> optionalUri) throws Throwable {
        StatsReceiver statsReceiver = statsReceiver();
        StatsProvider statsProvider =
                new CachingStatsProvider(new FinagleStatsProvider(statsReceiver));
        final DistributedLogServer server = new DistributedLogTwitterServerInternal(
                optionalUri,
                getOptionalFlag(confFlag),
                getOptionalFlag(streamConfFlag),
                getOptionalFlag(portFlag),
                Optional.<Integer>absent(),
                Optional.<Integer>absent(),
                Optional.<String>absent(),
                getOptionalFlag(muxFlag),
                statsReceiver,
                statsProvider);

        server.runServer();

        // register shutdown hook
        onExit(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                server.close();
                return BoxedUnit.UNIT;
            }
        });

        try {
            server.join();
        } catch (InterruptedException e) {
            logger.warn("Interrupted when waiting distributedlog server to be finished : ", e);
        }
        logger.info("DistributedLog Server Interrupted.");
        server.close();
        logger.info("Closed DistributedLog Server.");
    }

    /**
     * Wrapper to run DistributedLog Twitter Server
     */
    public static class Main {

        public static void main(String[] args) {
            new DistributedLogTwitterServer().main(args);
        }

    }
}
