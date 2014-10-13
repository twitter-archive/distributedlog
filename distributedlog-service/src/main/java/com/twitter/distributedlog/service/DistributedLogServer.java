package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.service.announcer.Announcer;
import com.twitter.distributedlog.service.announcer.NOPAnnouncer;
import com.twitter.distributedlog.service.announcer.ServerSetAnnouncer;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientIdRequiredFilter;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DistributedLogServer implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServer.class);

    final static String USAGE = "DistributedLogServer [-u <uri>] [-c <conf>]";
    final String[] args;
    final Options options = new Options();
    // expose finagle server stats.
    final StatsReceiver statsReceiver = new OstrichStatsReceiver();

    private DistributedLogServiceImpl dlService = null;
    private Server server = null;
    private StatsProvider statsProvider = null;
    private Announcer announcer = null;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);

    DistributedLogServer(String[] args) {
        this.args = args;
        // prepare options
        options.addOption("u", "uri", true, "DistributedLog URI");
        options.addOption("c", "conf", true, "DistributedLog Configuration File");
        options.addOption("s", "provider", true, "DistributedLog Stats Provider");
        options.addOption("p", "port", true, "DistributedLog Server Port");
        options.addOption("sp", "stats-port", true, "DistributedLog Stats Port");
        options.addOption("si", "shard-id", true, "DistributedLog Shard ID");
        options.addOption("a", "announce", true, "ServerSet Path to Announce");
    }

    void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(USAGE, options);
    }

    @Override
    public void run() {
        try {
            logger.info("Running distributedlog server");
            BasicParser parser = new BasicParser();
            CommandLine cmdline = parser.parse(options, args);
            runCmd(cmdline);
        } catch (ParseException pe) {
            printUsage();
            Runtime.getRuntime().exit(-1);
        } catch (IOException ie) {
            logger.error("Failed to start distributedlog server : ", ie);
            Runtime.getRuntime().exit(-1);
        }
    }

    void runCmd(CommandLine cmdline) throws ParseException, IOException {
        if (!cmdline.hasOption("u")) {
            throw new ParseException("No distributedlog uri provided.");
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
        statsProvider = new NullStatsProvider();
        if (cmdline.hasOption("s")) {
            String providerClass = cmdline.getOptionValue("s");
            statsProvider = ReflectionUtils.newInstance(providerClass, StatsProvider.class);
        }
        // !!! For Ostrich, it registered some non-daemon threads in ServiceTracker. If we don't stop
        //     service tracker, the proxy will just hang during shutdown.
        dlConf.setProperty("shouldShutdownServiceTracker", true);
        logger.info("Starting stats provider : {}", statsProvider.getClass());
        statsProvider.start(dlConf);

        int servicePort = Integer.parseInt(cmdline.getOptionValue("p", "0"));
        int statsPort = Integer.parseInt(cmdline.getOptionValue("sp", "0"));
        int shardId = Integer.parseInt(cmdline.getOptionValue("si", "0"));

        if (cmdline.hasOption("a")) {
            announcer = new ServerSetAnnouncer(
                    cmdline.getOptionValue("a"),
                    servicePort,
                    statsPort,
                    shardId);
        } else {
            announcer = new NOPAnnouncer();
        }

        Pair<DistributedLogServiceImpl, Server>
            serverPair = runServer(dlConf, uri, statsProvider, servicePort,
                  keepAliveLatch, statsReceiver);
        this.dlService = serverPair.getLeft();
        this.server = serverPair.getRight();

        // announce the service
        announcer.announce();
    }

    static Pair<DistributedLogServiceImpl, Server> runServer(
            DistributedLogConfiguration dlConf, URI dlUri, StatsProvider provider, int port) throws IOException {
        return runServer(dlConf, dlUri, provider, port,
                         new CountDownLatch(0), new NullStatsReceiver());
    }

    static Pair<DistributedLogServiceImpl, Server> runServer(
            DistributedLogConfiguration dlConf, URI dlUri, StatsProvider provider, int port,
            CountDownLatch keepAliveLatch, StatsReceiver statsReceiver) throws IOException {
        logger.info("Running server @ uri {}.", dlUri);
        // dl service
        DistributedLogServiceImpl dlService =
                new DistributedLogServiceImpl(dlConf, dlUri, provider.getStatsLogger(""), keepAliveLatch);

        StatsReceiver serviceStatsReceiver = statsReceiver.scope("service");
        StatsLogger serviceStatsLogger = provider.getStatsLogger("service");

        // starts dl server
        Server server = ServerBuilder.safeBuild(
                new ClientIdRequiredFilter<byte[], byte[]>(serviceStatsReceiver).andThen(
                    new StatsFilter<byte[], byte[]>(serviceStatsLogger).andThen(
                        new DistributedLogService.Service(dlService, new TBinaryProtocol.Factory()))),
                ServerBuilder.get()
                        .name("DistributedLogServer")
                        .codec(ThriftServerFramedCodec.get())
                        .reportTo(statsReceiver)
                        .bindTo(new InetSocketAddress(port)));
        logger.info("Started DistributedLog Server.");
        return Pair.of(dlService, server);
    }

    static void closeServer(Pair<DistributedLogServiceImpl, Server> pair) {
        if (null != pair.getLeft()) {
            pair.getLeft().shutdown();
        }
        if (null != pair.getRight()) {
            logger.info("Closing dl thrift server.");
            pair.getRight().close();
            logger.info("Closed dl thrift server.");
        }
    }

    /**
     * Close the server.
     */
    public void close() {
        if (null != announcer) {
            try {
                announcer.unannounce();
            } catch (IOException e) {
                logger.warn("Error on unannouncing service : ", e);
            }
            announcer.close();
        }
        closeServer(Pair.of(dlService, server));
        if (statsProvider != null) {
            statsProvider.stop();
        }
        keepAliveLatch.countDown();
    }

    public void join() throws InterruptedException {
        keepAliveLatch.await();
    }

    /**
     * Running distributedlog server.
     *
     * @param args
     *          distributedlog server args
     */
    public static DistributedLogServer run(String[] args) {
        final DistributedLogServer server = new DistributedLogServer(args);
        server.run();
        return server;
    }

    private static void closeServer(DistributedLogServer server) {
        server.close();
    }

    public static void main(String[] args) {
        final DistributedLogServer server = run(args);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Closing DistributedLog Server.");
                server.close();
                logger.info("Closed DistributedLog Server.");
            }
        });
        try {
            server.join();
        } catch (InterruptedException e) {
            logger.warn("Interrupted when waiting distributedlog server to be finished : ", e);
        }
        logger.info("DistributedLog Service Interrupted.");
        closeServer(server);
        logger.info("Closed DistributedLog Server.");
    }

}
