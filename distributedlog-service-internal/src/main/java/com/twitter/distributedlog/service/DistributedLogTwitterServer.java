package com.twitter.distributedlog.service;

import com.google.common.collect.Lists;
import com.twitter.app.Flag;
import com.twitter.app.Flaggable;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.server.AbstractTwitterServer;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.twitter.finagle.FinagleStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import java.util.Arrays;
import java.util.List;

/**
 * Twitter Server Based DistributedLog Write Proxy
 */
public class DistributedLogTwitterServer extends AbstractTwitterServer {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogTwitterServer.class);

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

    private final String[] dlArgs;

    private DistributedLogTwitterServer(String[] args) {
        List<String> argList = Lists.newArrayListWithExpectedSize(args.length);
        // filter out twitter-server's flags
        for (String arg : args) {
            if (arg.contains(".") && arg.contains("=")) {
                continue;
            }
            argList.add(arg);
        }
        this.dlArgs = argList.toArray(new String[argList.size()]);
    }

    @Override
    public void main() throws Throwable {
        StatsReceiver statsReceiver = statsReceiver();
        StatsProvider statsProvider = new FinagleStatsProvider(statsReceiver);
        final DistributedLogServer server =
                DistributedLogServer.run(dlArgs, statsReceiver, statsProvider);

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
            new DistributedLogTwitterServer(args).main(args);
        }

    }

    /**
     * Emulator to run DistributedLog Twitter Server
     */
    public static class Emulator {
        private final DistributedLogCluster dlCluster;
        private final String[] args;

        public Emulator(String[] args) throws Exception {
            this.dlCluster = DistributedLogCluster.newBuilder()
                    .numBookies(3)
                    .shouldStartZK(true)
                    .zkServers("127.0.0.1")
                    .shouldStartProxy(false)
                    .dlConf(new DistributedLogConfiguration())
                    .build();
            this.args = args;
        }

        public void start() throws Exception {
            dlCluster.start();

            // Run the server with dl cluster info
            String[] extendedArgs = new String[args.length + 2];
            System.arraycopy(args, 0, extendedArgs, 0, args.length);
            extendedArgs[extendedArgs.length - 2] = "-u";
            extendedArgs[extendedArgs.length - 1] = dlCluster.getUri().toString();
            logger.info("Using args {}", Arrays.toString(extendedArgs));
            Main.main(extendedArgs);
        }

        public void stop() throws Exception {
            dlCluster.stop();
        }

        public static void main(String[] args) throws Exception {
            Emulator emulator = null;
            try {
                emulator = new Emulator(args);
                emulator.start();
            } catch (Exception ex) {
                if (null != emulator) {
                    emulator.stop();
                }
                System.out.println("Exception occurred running emulator " + ex);
            }
        }
    }
}
