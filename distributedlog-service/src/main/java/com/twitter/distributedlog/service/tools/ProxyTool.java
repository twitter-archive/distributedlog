package com.twitter.distributedlog.service.tools;

import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common_internal.zookeeper.TwitterServerSet;
import com.twitter.common_internal.zookeeper.TwitterZk;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.service.ClientUtils;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.tools.Tool;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Tools to interact with proxies.
 */
public class ProxyTool extends Tool {

    static final Logger logger = LoggerFactory.getLogger(ProxyTool.class);

    private static Iterable<InetSocketAddress> getSdZkEndpointsForDC(String dc) {
        if ("atla".equals(dc)) {
            return TwitterZk.ATLA_SD_ZK_ENDPOINTS;
        } else if ("smf1".equals(dc)) {
            return TwitterZk.SMF1_SD_ZK_ENDPOINTS;
        } else {
            return TwitterZk.SD_ZK_ENDPOINTS;
        }
    }

    protected abstract static class ClusterCommand extends OptsCommand {

        protected Options options = new Options();
        protected String dc;
        protected TwitterServerSet.Service zkService;
        protected final List<String> streams = new ArrayList<String>();

        protected ClusterCommand(String name, String description) {
            super(name, description);
            options.addOption("s", "serverset", true, "DistributedLog Proxy ServerSet");
            options.addOption("r", "prefix", true, "Prefix of stream name. E.g. 'QuantumLeapTest-'.");
            options.addOption("e", "expression", true, "Expression to generate stream suffix. " +
                    "Currently we support range 'x-y', list 'x,y,z' and name 'xyz'");
        }

        @Override
        protected int runCmd(CommandLine commandLine) throws Exception {
            try {
                parseCommandLine(commandLine);
            } catch (ParseException pe) {
                println("ERROR: fail to parse commandline : '" + pe.getMessage() + "'");
                printUsage();
                return -1;
            }

            ZooKeeperClient zkClient = TwitterServerSet
                    .clientBuilder(zkService)
                    .zkEndpoints(getSdZkEndpointsForDC(dc))
                    .build();
            logger.info("Created zookeeper client for dc {} : {}", dc, zkService);
            try {
                ServerSet serverSet = TwitterServerSet.create(zkClient, zkService);
                DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                        .name("proxy_tool")
                        .clientId(ClientId$.MODULE$.apply("proxy_tool"))
                        .maxRedirects(2)
                        .serverSet(serverSet)
                        .clientBuilder(ClientBuilder.get()
                            .connectionTimeout(Duration.fromSeconds(2))
                            .tcpConnectTimeout(Duration.fromSeconds(2))
                            .requestTimeout(Duration.fromSeconds(10))
                            .hostConnectionLimit(1)
                            .hostConnectionCoresize(1)
                            .keepAlive(true)
                            .failFast(false))
                        .build();
                try {
                    return runCmd(client);
                } finally {
                    client.close();
                }
            } finally {
                zkClient.close();
            }
        }

        protected abstract int runCmd(DistributedLogClient client) throws Exception;

        @Override
        protected Options getOptions() {
            return options;
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            if (!cmdline.hasOption("s")) {
                throw new ParseException("No proxy serverset provided.");
            }
            String[] serverSetPathParts = StringUtils.split(cmdline.getOptionValue("s"), '/');
            if (serverSetPathParts.length != 4) {
                throw new ParseException("Invalid serverset path : " + cmdline.getOptionValue("s")
                        + ", Expected: <dc>/<role>/<env>/<job>.");
            }
            dc = serverSetPathParts[0];
            zkService = new TwitterServerSet.Service(serverSetPathParts[1], serverSetPathParts[2], serverSetPathParts[3]);

            // get stream names
            String streamPrefix = cmdline.hasOption("r") ? cmdline.getOptionValue("r") : "";
            String streamExpression = null;
            if (cmdline.hasOption("e")) {
                streamExpression = cmdline.getOptionValue("e");
            }
            if (null == streamPrefix || null == streamExpression) {
                throw new ParseException("Please specify stream prefix & expression.");
            }
            // parse the stream expression
            if (streamExpression.contains("-")) {
                // a range expression
                String[] parts = streamExpression.split("-");
                if (parts.length != 2) {
                    throw new ParseException("Invalid stream index range : " + streamExpression);
                }
                try {
                    int start = Integer.parseInt(parts[0]);
                    int end = Integer.parseInt(parts[1]);
                    if (start > end) {
                        throw new ParseException("Invalid stream index range : " + streamExpression);
                    }
                    for (int i = start; i <= end; i++) {
                        streams.add(streamPrefix + i);
                    }
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid stream index range : " + streamExpression);
                }
            } else if (streamExpression.contains(",")) {
                // a list expression
                String[] parts = streamExpression.split(",");
                try {
                    for (String part : parts) {
                        streams.add(streamPrefix + part);
                    }
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid stream suffix list : " + streamExpression);
                }
            } else {
                streams.add(streamPrefix + streamExpression);
            }
        }
    }

    static class ReleaseCommand extends ClusterCommand {

        double rate = 100f;

        ReleaseCommand() {
            super("release", "Release Stream Ownerships");
            options.addOption("t", "rate", true, "Rate to release streams");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("t")) {
                rate = Double.parseDouble(cmdline.getOptionValue("t", "100"));
            }
        }

        @Override
        protected int runCmd(DistributedLogClient client) throws Exception {
            RateLimiter rateLimiter = RateLimiter.create(rate);
            for (String stream : streams) {
                rateLimiter.acquire();
                try {
                    Await.result(client.release(stream));
                    println("Release ownership of stream " + stream);
                } catch (Exception e) {
                    println("Failed to release ownership of stream " + stream);
                    throw e;
                }
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "release [options]";
        }
    }

    static class TruncateCommand extends ClusterCommand {

        DLSN dlsn = DLSN.InitialDLSN;

        TruncateCommand() {
            super("truncate", "Truncate streams until given dlsn.");
            options.addOption("d", "dlsn", true, "DLSN to truncate until");
        }

        @Override
        protected int runCmd(DistributedLogClient client) throws Exception {
            println("Truncating streams : " + streams);
            for (String stream : streams) {
                boolean success = Await.result(client.truncate(stream, dlsn));
                println("Truncate " + stream + " to " + dlsn + " : " + success);
            }
            return 0;
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("d")) {
                throw new ParseException("No DLSN provided");
            }
            String[] dlsnStrs = cmdline.getOptionValue("d").split(",");
            if (dlsnStrs.length != 3) {
                throw new ParseException("Invalid DLSN : " + cmdline.getOptionValue("d"));
            }
            dlsn = new DLSN(Long.parseLong(dlsnStrs[0]), Long.parseLong(dlsnStrs[1]), Long.parseLong(dlsnStrs[2]));
        }

        @Override
        protected String getUsage() {
            return "truncate [options]";
        }
    }

    protected abstract static class ProxyCommand extends OptsCommand {

        protected Options options = new Options();
        protected InetSocketAddress address;

        protected ProxyCommand(String name, String description) {
            super(name, description);
            options.addOption("H", "host", true, "Single Proxy Address");
        }

        @Override
        protected Options getOptions() {
            return options;
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            if (!cmdline.hasOption("H")) {
                throw new ParseException("No proxy address provided");
            }
            address = DLSocketAddress.parseSocketAddress(cmdline.getOptionValue("H"));
        }

        @Override
        protected int runCmd(CommandLine commandLine) throws Exception {
            try {
                parseCommandLine(commandLine);
            } catch (ParseException pe) {
                println("ERROR: fail to parse commandline : '" + pe.getMessage() + "'");
                printUsage();
                return -1;
            }

            DistributedLogClientBuilder clientBuilder = DistributedLogClientBuilder.newBuilder()
                    .name("proxy_tool")
                    .clientId(ClientId$.MODULE$.apply("proxy_tool"))
                    .maxRedirects(2)
                    .host(address)
                    .clientBuilder(ClientBuilder.get()
                            .connectionTimeout(Duration.fromSeconds(2))
                            .tcpConnectTimeout(Duration.fromSeconds(2))
                            .requestTimeout(Duration.fromSeconds(10))
                            .hostConnectionLimit(1)
                            .hostConnectionCoresize(1)
                            .keepAlive(true)
                            .failFast(false));
            Pair<DistributedLogClient, MonitorServiceClient> clientPair =
                    ClientUtils.buildClient(clientBuilder);
            try {
                return runCmd(clientPair);
            } finally {
                clientPair.getLeft().close();
            }
        }

        protected abstract int runCmd(Pair<DistributedLogClient, MonitorServiceClient> client) throws Exception;
    }

    static class AcceptNewStreamCommand extends ProxyCommand {

        boolean enabled = false;

        AcceptNewStreamCommand() {
            super("accept-new-stream", "Enable/Disable accepting new streams for one proxy");
            options.addOption("e", "enabled", true, "Enable/Disable accepting new streams");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("e")) {
                throw new ParseException("No action 'enable/disable' provided");
            }
            enabled = Boolean.parseBoolean(cmdline.getOptionValue("e"));
        }

        @Override
        protected int runCmd(Pair<DistributedLogClient, MonitorServiceClient> client)
                throws Exception {
            Await.result(client.getRight().setAcceptNewStream(enabled));
            return 0;
        }

        @Override
        protected String getUsage() {
            return "accept-new-stream [options]";
        }
    }

    public ProxyTool() {
        super();
        addCommand(new ReleaseCommand());
        addCommand(new TruncateCommand());
        addCommand(new AcceptNewStreamCommand());
    }

    @Override
    protected String getName() {
        return "proxy_tool";
    }
}
