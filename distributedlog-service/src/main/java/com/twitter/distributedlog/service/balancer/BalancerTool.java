/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.distributedlog.service.ClientUtils;
import com.twitter.distributedlog.service.DLSocketAddress;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.tools.Tool;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId$;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * Tool to rebalance cluster
 */
public class BalancerTool extends Tool {

    static final Logger logger = LoggerFactory.getLogger(BalancerTool.class);

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

        protected URI uri;
        protected String source = null;

        protected ClusterBalancerCommand() {
            super("clusterbalancer", "Balance streams inside a cluster");
            options.addOption("u", "uri", true, "DistributedLog URI");
            options.addOption("sp", "source-proxy", true, "Source proxy to balance");
        }

        @Override
        protected String getUsage() {
            return "clusterbalancer [options]";
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("u")) {
                throw new ParseException("No proxy serverset provided.");
            }
            uri = URI.create(cmdline.getOptionValue("u"));
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
            DLZkServerSet serverSet = DLZkServerSet.of(uri, 60000);
            logger.info("Created serverset for {}", uri);
            try {
                DistributedLogClientBuilder clientBuilder =
                        createDistributedLogClientBuilder(serverSet.getServerSet());
                ClusterBalancer balancer = new ClusterBalancer(clientBuilder);
                try {
                    return runBalancer(clientBuilder, balancer);
                } finally {
                    balancer.close();
                }
            } finally {
                serverSet.close();
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
                            .host(sourceAddr);

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

        protected URI region1;
        protected URI region2;
        protected String source = null;

        protected RegionBalancerCommand() {
            super("regionbalancer", "Balance streams between regions");
            options.addOption("rs", "regions", true, "DistributedLog Region URI: uri1[,uri2]");
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
            region1 = URI.create(regions[0]);
            region2 = URI.create(regions[1]);
            if (cmdline.hasOption("s")) {
                source = cmdline.getOptionValue("s");
            }
        }

        @Override
        protected int executeCommand(CommandLine cmdline) throws Exception {
            DLZkServerSet serverSet1 = DLZkServerSet.of(region1, 60000);
            logger.info("Created serverset for {}", region1);
            DLZkServerSet serverSet2 = DLZkServerSet.of(region2, 60000);
            logger.info("Created serverset for {}", region2);
            try {
                DistributedLogClientBuilder builder1 =
                        createDistributedLogClientBuilder(serverSet1.getServerSet());
                Pair<DistributedLogClient, MonitorServiceClient> pair1 =
                        ClientUtils.buildClient(builder1);
                DistributedLogClientBuilder builder2 =
                        createDistributedLogClientBuilder(serverSet2.getServerSet());
                Pair<DistributedLogClient, MonitorServiceClient> pair2 =
                        ClientUtils.buildClient(builder2);
                try {
                    SimpleBalancer balancer = new SimpleBalancer(
                            DLUtils.getZKServersFromDLUri(region1), pair1.getLeft(), pair1.getRight(),
                            DLUtils.getZKServersFromDLUri(region2), pair2.getLeft(), pair2.getRight());
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
                serverSet1.close();
                serverSet2.close();
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
