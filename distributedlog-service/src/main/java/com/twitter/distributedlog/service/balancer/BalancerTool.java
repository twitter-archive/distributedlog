package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common_internal.zookeeper.TwitterServerSet;
import com.twitter.common_internal.zookeeper.TwitterZk;
import com.twitter.distributedlog.service.ClientUtils;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.service.MonitorServiceClient;
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

/**
 * Tool to rebalance cluster
 */
public class BalancerTool extends Tool {

    static final Logger logger = LoggerFactory.getLogger(BalancerTool.class);

    private static Iterable<InetSocketAddress> getSdZkEndpointsForDC(String dc) {
        if ("atla".equals(dc)) {
            return TwitterZk.ATLA_SD_ZK_ENDPOINTS;
        } else if ("smf1".equals(dc)) {
            return TwitterZk.SMF1_SD_ZK_ENDPOINTS;
        } else {
            return TwitterZk.SD_ZK_ENDPOINTS;
        }
    }

    static Pair<String, TwitterServerSet.Service> parseServerSet(String serverSetPath) throws ParseException {
        String[] serverSetPathParts = StringUtils.split(serverSetPath, '/');
        if (serverSetPathParts.length != 4) {
            throw new ParseException("Invalid serverset path : " + serverSetPath
                    + ", Expected: <dc>/<role>/<env>/<job>.");
        }
        String dc = serverSetPathParts[0];
        TwitterServerSet.Service zkService =
                new TwitterServerSet.Service(serverSetPathParts[1], serverSetPathParts[2], serverSetPathParts[3]);
        return Pair.of(dc, zkService);
    }

    static DistributedLogClientBuilder createDistributedLogClientBuilder(ServerSet serverSet) {
        return DistributedLogClientBuilder.newBuilder()
                        .name("rebalancer_tool")
                        .clientId(ClientId$.MODULE$.apply("rebalancer_tool"))
                        .maxRedirects(2)
                        .serverSet(serverSet)
                        .clientBuilder(ClientBuilder.get()
                                .connectionTimeout(Duration.fromSeconds(2))
                                .tcpConnectTimeout(Duration.fromSeconds(2))
                                .requestTimeout(Duration.fromSeconds(10))
                                .hostConnectionLimit(1)
                                .hostConnectionCoresize(1)
                                .keepAlive(true)
                                .failFast(false));
    }

    protected abstract static class BalancerCommand extends OptsCommand {

        protected Options options = new Options();
        protected int rebalanceWaterMark = 0;
        protected double rebalanceTolerancePercentage = 0.0f;
        protected int rebalanceConcurrency = 1;
        protected Double rate = null;
        protected Optional<RateLimiter> rateLimiter;

        BalancerCommand(String name, String description) {
            super(name, description);
            options.addOption("rwm", "rebalance-water-mark", true, "Rebalance water mark per proxy");
            options.addOption("rtp", "rebalance-tolerance-percentage", true, "Rebalance tolerance percentage per proxy");
            options.addOption("rc", "rebalance-concurrency", true, "Concurrency to rebalance stream distribution");
            options.addOption("r", "rate", true, "Rebalance rate");
        }

        Optional<RateLimiter> getRateLimiter() {
            return rateLimiter;
        }

        @Override
        protected Options getOptions() {
            return options;
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            if (cmdline.hasOption("rwm")) {
                this.rebalanceWaterMark = Integer.parseInt(cmdline.getOptionValue("rwm"));
            }
            if (cmdline.hasOption("rtp")) {
                this.rebalanceTolerancePercentage = Double.parseDouble(cmdline.getOptionValue("rtp"));
            }
            if (cmdline.hasOption("rc")) {
                this.rebalanceConcurrency = Integer.parseInt(cmdline.getOptionValue("rc"));
            }
            if (cmdline.hasOption("r")) {
                this.rate = Double.parseDouble(cmdline.getOptionValue("r"));
            }
            Preconditions.checkArgument(rebalanceWaterMark >= 0,
                    "Rebalance Water Mark should be a non-negative number");
            Preconditions.checkArgument(rebalanceTolerancePercentage >= 0.0f,
                    "Rebalance Tolerance Percentage should be a non-negative number");
            Preconditions.checkArgument(rebalanceConcurrency > 0,
                    "Rebalance Concurrency should be a positive number");
            if (null == rate || rate <= 0.0f) {
                rateLimiter = Optional.absent();
            } else {
                rateLimiter = Optional.of(RateLimiter.create(rate));
            }
        }

        @Override
        protected int runCmd(CommandLine cmdline) throws Exception {
            try {
                parseCommandLine(cmdline);
            } catch (ParseException pe) {
                println("ERROR: fail to parse commandline : '" + pe.getMessage() + "'");
                printUsage();
                return -1;
            }
            return executeCommand(cmdline);
        }

        protected abstract int executeCommand(CommandLine cmdline) throws Exception;
    }

    protected static class ClusterBalancerCommand extends BalancerCommand {

        protected Pair<String, TwitterServerSet.Service> cluster;
        protected String source = null;

        protected ClusterBalancerCommand() {
            super("clusterbalancer", "Balance streams inside a cluster");
            options.addOption("s", "serverset", true, "DistributedLog Proxy ServerSet");
            options.addOption("sp", "source-proxy", true, "Source proxy to balance");
        }

        @Override
        protected String getUsage() {
            return "clusterbalancer [options]";
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("s")) {
                throw new ParseException("No proxy serverset provided.");
            }
            cluster = parseServerSet(cmdline.getOptionValue("s"));
            if (cmdline.hasOption("sp")) {
                String sourceProxyStr = cmdline.getOptionValue("sp");
                try {
                    DLSocketAddress.parseSocketAddress(sourceProxyStr);
                } catch (IllegalArgumentException iae) {
                    throw new ParseException("Invalid source proxy " + sourceProxyStr + " : " + iae.getMessage());
                }
                this.source = sourceProxyStr;
            }
        }

        @Override
        protected int executeCommand(CommandLine cmdline) throws Exception {
            ZooKeeperClient zkClient = TwitterServerSet
                    .clientBuilder(cluster.getRight())
                    .zkEndpoints(getSdZkEndpointsForDC(cluster.getLeft()))
                    .build();
            logger.info("Created zookeeper client for dc {} : {}",
                    cluster.getLeft(), cluster.getRight());
            try {
                ServerSet serverSet = TwitterServerSet.create(zkClient, cluster.getRight());
                DistributedLogClientBuilder clientBuilder = createDistributedLogClientBuilder(serverSet);
                ClusterBalancer balancer = new ClusterBalancer(clientBuilder);
                try {
                    return runBalancer(clientBuilder, balancer);
                } finally {
                    balancer.close();
                }
            } finally {
                zkClient.close();
            }
        }

        protected int runBalancer(DistributedLogClientBuilder clientBuilder,
                                  ClusterBalancer balancer)
                throws Exception {
            if (null == source) {
                balancer.balance(rebalanceWaterMark, rebalanceTolerancePercentage, rebalanceConcurrency, getRateLimiter());
            } else {
                balanceFromSource(clientBuilder, balancer, source, getRateLimiter());
            }
            return 0;
        }

        protected void balanceFromSource(DistributedLogClientBuilder clientBuilder,
                                         ClusterBalancer balancer,
                                         String source,
                                         Optional<RateLimiter> rateLimiter)
                throws Exception {
            InetSocketAddress sourceAddr = DLSocketAddress.parseSocketAddress(source);
            DistributedLogClientBuilder sourceClientBuilder =
                    DistributedLogClientBuilder.newBuilder(clientBuilder)
                            .routingService(new SingleHostRoutingService(sourceAddr));

            Pair<DistributedLogClient, MonitorServiceClient> clientPair =
                    ClientUtils.buildClient(sourceClientBuilder);
            try {
                Await.result(clientPair.getRight().setAcceptNewStream(false));
                logger.info("Disable accepting new stream on proxy {}.", source);
                balancer.balanceAll(source, rebalanceConcurrency, rateLimiter);
            } finally {
                clientPair.getLeft().close();
            }
        }
    }

    protected static class RegionBalancerCommand extends BalancerCommand {

        protected Pair<String, TwitterServerSet.Service> region1;
        protected Pair<String, TwitterServerSet.Service> region2;
        protected String source = null;

        protected RegionBalancerCommand() {
            super("regionbalancer", "Balance streams between regions");
            options.addOption("rs", "regions", true, "DistributedLog Region ServerSets: serverset1[,serverset2]");
            options.addOption("s", "source", true, "DistributedLog Source Region to balance");
        }

        @Override
        protected String getUsage() {
            return "regionbalancer [options]";
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("rs")) {
                throw new ParseException("No regions provided.");
            }
            String regionsStr = cmdline.getOptionValue("rs");
            String[] regions = regionsStr.split(",");
            if (regions.length != 2) {
                throw new ParseException("Invalid regions provided. Expected : serverset1[,serverset2]");
            }
            region1 = parseServerSet(regions[0]);
            region2 = parseServerSet(regions[1]);
            if (cmdline.hasOption("s")) {
                source = cmdline.getOptionValue("s");
            }
        }

        @Override
        protected int executeCommand(CommandLine cmdline) throws Exception {
            ZooKeeperClient zkClient1 = TwitterServerSet
                    .clientBuilder(region1.getRight())
                    .zkEndpoints(getSdZkEndpointsForDC(region1.getLeft()))
                    .build();
            logger.info("Created zookeeper client for dc {} : {}",
                    region1.getLeft(), region1.getRight());
            ZooKeeperClient zkClient2 = TwitterServerSet
                    .clientBuilder(region2.getRight())
                    .zkEndpoints(getSdZkEndpointsForDC(region2.getLeft()))
                    .build();
            logger.info("Created zookeeper client for dc {} : {}",
                        region2.getLeft(), region2.getRight());
            try {
                ServerSet serverSet1 = TwitterServerSet.create(zkClient1, region1.getRight());
                DistributedLogClientBuilder builder1 = createDistributedLogClientBuilder(serverSet1);
                Pair<DistributedLogClient, MonitorServiceClient> pair1 =
                        ClientUtils.buildClient(builder1);
                ServerSet serverSet2 = TwitterServerSet.create(zkClient2, region2.getRight());
                DistributedLogClientBuilder builder2 = createDistributedLogClientBuilder(serverSet2);
                Pair<DistributedLogClient, MonitorServiceClient> pair2 =
                        ClientUtils.buildClient(builder2);
                try {
                    SimpleBalancer balancer = new SimpleBalancer(
                            region1.getLeft(), pair1.getLeft(), pair1.getRight(),
                            region2.getLeft(), pair2.getLeft(), pair2.getRight());
                    try {
                        return runBalancer(balancer);
                    } finally {
                        balancer.close();
                    }
                } finally {
                    pair1.getLeft().close();
                    pair2.getLeft().close();
                }
            } finally {
                zkClient1.close();
                zkClient2.close();
            }
        }

        protected int runBalancer(SimpleBalancer balancer) throws Exception {
            if (null == source) {
                balancer.balance(rebalanceWaterMark, rebalanceTolerancePercentage, rebalanceConcurrency, getRateLimiter());
            } else {
                balancer.balanceAll(source, rebalanceConcurrency, getRateLimiter());
            }
            return 0;
        }
    }

    public BalancerTool() {
        super();
        addCommand(new ClusterBalancerCommand());
        addCommand(new RegionBalancerCommand());
    }

    @Override
    protected String getName() {
        return "balancer";
    }
}
