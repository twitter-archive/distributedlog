package com.twitter.distributedlog.service;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientIdRequiredFilter;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;

public class DistributedLogServer implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogServer.class);

    final static String USAGE = "DistributedLogServer [-u <uri>] [-c <conf>]";
    final String[] args;
    final Options options = new Options();
    // expose finagle server stats.
    final StatsReceiver statsReceiver = new OstrichStatsReceiver();

    private DistributedLogServiceImpl dlService = null;
    private Server server = null;

    DistributedLogServer(String[] args) {
        this.args = args;
        // prepare options
        options.addOption("u", "uri", true, "DistributedLog URI");
        options.addOption("c", "conf", true, "DistributedLog Configuration File");
        options.addOption("s", "provider", true, "DistributedLog Stats Provider");
        options.addOption("p", "port", true, "DistributedLog Server Port");
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
        StatsProvider provider = new NullStatsProvider();
        if (cmdline.hasOption("s")) {
            String providerClass = cmdline.getOptionValue("s");
            provider = ReflectionUtils.newInstance(providerClass, StatsProvider.class);
        }
        runServer(dlConf, uri, provider,
                  Integer.parseInt(cmdline.getOptionValue("p", "0")));
    }

    void runServer(DistributedLogConfiguration dlConf, URI dlUri, StatsProvider provider, int port) throws IOException {
        logger.info("Running server @ uri {}.", dlUri);
        // dl service
        dlService = new DistributedLogServiceImpl(dlConf, dlUri, provider.getStatsLogger(""));

        // starts dl server
        server = ServerBuilder.safeBuild(
                new ClientIdRequiredFilter<byte[], byte[]>(statsReceiver.scope("service")).andThen(
                    new DistributedLogService.Service(dlService, new TBinaryProtocol.Factory())),
                ServerBuilder.get()
                        .name("DistributedLogServer")
                        .codec(ThriftServerFramedCodec.get())
                        .reportTo(statsReceiver)
                        .bindTo(new InetSocketAddress(port)));
        logger.info("Started DistributedLog Server.");
    }

    /**
     * Close the server.
     */
    public void close() {
        if (null != server) {
            server.close();
        }
        if (null != dlService) {
            dlService.shutdown();
        }
    }

    /**
     * Running distributedlog server.
     *
     * @param args
     *          distributedlog server args
     */
    public static void main(String[] args) {
        final DistributedLogServer server = new DistributedLogServer(args);
        server.run();
        final CountDownLatch keepAliveLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Closing DistributedLog Server.");
                server.close();
                logger.info("Closed DistributedLog Server.");
                keepAliveLatch.countDown();
            }
        });
        try {
            keepAliveLatch.await();
        } catch (InterruptedException e) {
            logger.info("Quit DistributedLog Server : ", e);
        }
    }

}
