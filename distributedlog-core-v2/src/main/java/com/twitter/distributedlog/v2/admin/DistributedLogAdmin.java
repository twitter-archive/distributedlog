package com.twitter.distributedlog.v2.admin;

import com.twitter.distributedlog.v2.ZooKeeperClient;
import com.twitter.distributedlog.v2.ZooKeeperClientBuilder;
import com.twitter.distributedlog.v2.metadata.BKDLConfig;
import com.twitter.distributedlog.v2.metadata.DLMetadata;
import com.twitter.distributedlog.v2.tools.Tool;
import com.twitter.distributedlog.v2.util.DLUtils;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.URI;

/**
 * Admin Tool for DistributedLog.
 */
public class DistributedLogAdmin extends Tool {

    /**
     * Unbind the bookkeeper environment for a given distributedlog uri.
     */
    class UnbindCommand extends OptsCommand {

        Options options = new Options();

        UnbindCommand() {
            super("unbind", "unbind the bookkeeper environment bound for a given distributedlog instance.");
            options.addOption("f", "force", false, "Force unbinding without prompt.");
        }

        @Override
        protected Options getOptions() {
            return options;
        }

        @Override
        protected String getUsage() {
            return "unbind [options] <distributedlog uri>";
        }

        @Override
        protected int runCmd(CommandLine cmdline) throws Exception {
            String[] args = cmdline.getArgs();
            if (args.length <= 0) {
                println("No distributedlog uri specified.");
                printUsage();
                return -1;
            }
            boolean force = cmdline.hasOption("f");
            URI uri = URI.create(args[0]);
            // resolving the uri to see if there is another bindings in this uri.
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder().uri(uri)
                    .sessionTimeoutMs(10000).build();
            BKDLConfig bkdlConfig;
            try {
                bkdlConfig = BKDLConfig.resolveDLConfig(zkc, uri);
            } catch (IOException ie) {
                bkdlConfig = null;
            }
            if (null == bkdlConfig) {
                println("No bookkeeper is bound for " + uri);
                return 0;
            } else {
                println("There is bookkeeper bound for " + uri + " : ");
                println("");
                println(bkdlConfig.toString());
                println("");
                if (!force && !IOUtils.confirmPrompt("Are you sure to unbind " + uri + " :\n")) {
                    println("You just gave up. ByeBye.");
                    return 0;
                }
            }
            DLMetadata.unbind(uri);
            println("Unbound on " + uri + ".");
            return 0;
        }
    }

    /**
     * Bind Command to bind bookkeeper environment for a given distributed uri.
     */
    class BindCommand extends OptsCommand {

        Options options = new Options();

        BindCommand() {
            super("bind", "bind the bookkeeper environment settings for a given distributedlog instance.");
            options.addOption("l", "bkLedgers", true, "ZooKeeper ledgers path for bookkeeper instance.");
            options.addOption("s", "bkZkServers", true, "ZooKeeper servers used for bookkeeper for writers.");
            options.addOption("bkzr", "bkZkServersForReader", true, "ZooKeeper servers used for bookkeeper for readers.");
            options.addOption("dlzw", "dlZkServersForWriter", true, "ZooKeeper servers used for distributedlog for writers.");
            options.addOption("dlzr", "dlZkServersForReader", true, "ZooKeeper servers used for distributedlog for readers.");
            options.addOption("i", "sanityCheckTxnID", true, "Flag to sanity check highest txn id.");
            options.addOption("r", "encodeRegionID", true, "Flag to encode region id.");
            options.addOption("f", "force", false, "Force binding without prompt.");
            options.addOption("c", "creation", false, "Whether is it a creation binding.");
            options.addOption("q", "query", false, "Query the bookkeeper bindings");
        }

        @Override
        protected Options getOptions() {
            return options;
        }

        @Override
        protected String getUsage() {
            return "bind [options] <distributedlog uri>";
        }

        @Override
        protected int runCmd(CommandLine cmdline) throws Exception {
            boolean isQuery = cmdline.hasOption("q");
            if (!isQuery && (!cmdline.hasOption("l") || !cmdline.hasOption("s"))) {
                println("Error: Neither zkServers nor ledgersPath specified for bookkeeper environment.");
                printUsage();
                return -1;
            }
            String[] args = cmdline.getArgs();
            if (args.length <= 0) {
                println("No distributedlog uri specified.");
                printUsage();
                return -1;
            }
            boolean force = cmdline.hasOption("f");
            boolean creation = cmdline.hasOption("c");
            String bkLedgersPath = cmdline.getOptionValue("l");
            String bkZkServersForWriter = cmdline.getOptionValue("s");
            boolean sanityCheckTxnID =
                    !cmdline.hasOption("i") || Boolean.parseBoolean(cmdline.getOptionValue("i"));
            boolean encodeRegionID =
                    cmdline.hasOption("r") && Boolean.parseBoolean(cmdline.getOptionValue("r"));

            String bkZkServersForReader;
            if (cmdline.hasOption("bkzr")) {
                bkZkServersForReader = cmdline.getOptionValue("bkzr");
            } else {
                bkZkServersForReader = bkZkServersForWriter;
            }

            URI uri = URI.create(args[0]);

            String dlZkServersForWriter;
            String dlZkServersForReader;
            if (cmdline.hasOption("dlzw")) {
                dlZkServersForWriter = cmdline.getOptionValue("dlzw");
            } else {
                dlZkServersForWriter = DLUtils.getZKServersFromDLUri(uri);
            }
            if (cmdline.hasOption("dlzr")) {
                dlZkServersForReader = cmdline.getOptionValue("dlzr");
            } else {
                dlZkServersForReader = dlZkServersForWriter;
            }

            // resolving the uri to see if there is another bindings in this uri.
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder().uri(uri)
                    .sessionTimeoutMs(10000).build();
            try {
                BKDLConfig newBKDLConfig =
                        new BKDLConfig(dlZkServersForWriter, dlZkServersForReader,
                                       bkZkServersForWriter, bkZkServersForReader, bkLedgersPath)
                                .setSanityCheckTxnID(sanityCheckTxnID)
                                .setEncodeRegionID(encodeRegionID);
                BKDLConfig bkdlConfig;
                try {
                    bkdlConfig = BKDLConfig.resolveDLConfig(zkc, uri);
                } catch (IOException ie) {
                    bkdlConfig = null;
                }
                if (null == bkdlConfig) {
                    println("No bookkeeper is bound for " + uri);
                } else {
                    println("There is bookkeeper bound for " + uri + " : ");
                    println("");
                    println(bkdlConfig.toString());
                    println("");
                    if (!isQuery) {
                        if (newBKDLConfig.equals(bkdlConfig)) {
                            println("No bookkeeper binding needs to be updated. Quit.");
                            return 0;
                        } else {
                            if (!force && !IOUtils.confirmPrompt("Are you sure to bind " + uri
                                        + " with new bookkeeper instance :\n" + newBKDLConfig)) {
                                println("You just gave up. ByeBye.");
                                return 0;
                            }
                        }
                    }
                }
                if (isQuery) {
                    println("Done.");
                    return 0;
                }
                DLMetadata dlMetadata = DLMetadata.create(newBKDLConfig);
                if (creation) {
                    try {
                        dlMetadata.create(uri);
                        println("Created binding on " + uri + ".");
                    } catch (IOException ie) {
                        println("Failed to create binding on " + uri + " : " + ie.getMessage());
                    }
                } else {
                    try {
                        dlMetadata.update(uri);
                        println("Updated binding on " + uri + " : ");
                        println("");
                        println(newBKDLConfig.toString());
                        println("");
                    } catch (IOException ie) {
                        println("Failed to update binding on " + uri + " : " + ie.getMessage());
                    }
                }
                return 0;
            } finally {
                zkc.close();
            }
        }
    }

    public DistributedLogAdmin() {
        super();
        addCommand(new BindCommand());
        addCommand(new UnbindCommand());
    }

    @Override
    protected String getName() {
        return "dlog_admin";
    }
}
