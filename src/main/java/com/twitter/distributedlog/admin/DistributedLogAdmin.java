package com.twitter.distributedlog.admin;

import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.twitter.distributedlog.metadata.DryrunZkMetadataUpdater;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import com.twitter.distributedlog.tools.DistributedLogTool;
import com.twitter.distributedlog.tools.Tool;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Admin Tool for DistributedLog.
 */
public class DistributedLogAdmin extends DistributedLogTool {

    static final Logger LOG = LoggerFactory.getLogger(DistributedLogAdmin.class);

    /**
     * Fix inprogress segment with lower ledger sequence number.
     *
     * @param factory
     *          dlm factory.
     * @param metadataUpdater
     *          metadata updater.
     * @param streamName
     *          stream name.
     * @param verbose
     *          print verbose messages.
     * @param interactive
     *          is confirmation needed before executing actual action.
     * @throws IOException
     */
    public static void fixInprogressSegmentWithLowerSequenceNumber(final DistributedLogManagerFactory factory,
                                                                   final MetadataUpdater metadataUpdater,
                                                                   final String streamName,
                                                                   final boolean verbose,
                                                                   final boolean interactive) throws IOException {
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        try {
            List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
            if (verbose) {
                System.out.println("LogSegments for " + streamName + " : ");
                for (LogSegmentLedgerMetadata segment : segments) {
                    System.out.println(segment.getLedgerSequenceNumber() + "\t: " + segment);
                }
            }
            LOG.info("Get log segments for {} : {}", streamName, segments);
            // validate log segments
            long maxCompletedLedgerSequenceNumber = -1L;
            LogSegmentLedgerMetadata inprogressSegment = null;
            for (LogSegmentLedgerMetadata segment : segments) {
                if (!segment.isInProgress()) {
                    maxCompletedLedgerSequenceNumber = Math.max(maxCompletedLedgerSequenceNumber, segment.getLedgerSequenceNumber());
                } else {
                    // we already found an inprogress segment
                    if (null != inprogressSegment) {
                        throw new DLIllegalStateException("Multiple inprogress segments found for stream " + streamName + " : " + segments);
                    }
                    inprogressSegment = segment;
                }
            }
            if (null == inprogressSegment || inprogressSegment.getLedgerSequenceNumber() > maxCompletedLedgerSequenceNumber) {
                // nothing to fix
                return;
            }
            final long newLedgerSequenceNumber = maxCompletedLedgerSequenceNumber + 1;
            if (interactive && !IOUtils.confirmPrompt("Confirm to fix (Y/N), Ctrl+C to break : ")) {
                if (verbose) {
                    System.out.println("Give up fixing stream " + streamName);
                }
                return;
            }
            final LogSegmentLedgerMetadata newSegment =
                    metadataUpdater.changeSequenceNumber(inprogressSegment, newLedgerSequenceNumber);
            LOG.info("Fixed {} : {} -> {} ",
                     new Object[] { streamName, inprogressSegment, newSegment });
            if (verbose) {
                System.out.println("Fixed " + streamName + " : " + inprogressSegment.getZNodeName()
                                   + " -> " + newSegment.getZNodeName());
                System.out.println("\t old: " + inprogressSegment);
                System.out.println("\t new: " + newSegment);
                System.out.println();
            }
        } finally {
            dlm.close();
        }
    }

    //
    // Commands
    //

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
            options.addOption("s", "bkZkServers", true, "ZooKeeper servers used for bookkeeper instance.");
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
            String bkZkServers = cmdline.getOptionValue("s");
            boolean sanityCheckTxnID =
                    !cmdline.hasOption("i") || Boolean.parseBoolean(cmdline.getOptionValue("i"));
            boolean encodeRegionID =
                    cmdline.hasOption("r") && Boolean.parseBoolean(cmdline.getOptionValue("r"));
            URI uri = URI.create(args[0]);
            // resolving the uri to see if there is another bindings in this uri.
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder().uri(uri)
                    .sessionTimeoutMs(10000).build();
            try {
                BKDLConfig newBKDLConfig =
                        new BKDLConfig(bkZkServers, bkLedgersPath)
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

    class RepairSeqNoCommand extends PerDLCommand {

        boolean dryrun = false;
        boolean verbose = false;
        boolean force = false;
        final List<String> streams = new ArrayList<String>();

        RepairSeqNoCommand() {
            super("repairseqno", "Repair a stream whose inprogress log segment has lower sequence number.");
            options.addOption("d", "dryrun", false, "Dry run without repairing");
            options.addOption("l", "list", true, "List of streams to repair, separated by comma");
            options.addOption("v", "verbose", false, "Print verbose messages");
            options.addOption("f", "force", false, "Force execution without confirmation execution");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            dryrun = cmdline.hasOption("d");
            verbose = cmdline.hasOption("v");
            force = !dryrun && cmdline.hasOption("f");
            if (!cmdline.hasOption("l")) {
                throw new ParseException("No streams provided to repair");
            }
            String streamsList = cmdline.getOptionValue("l");
            Collections.addAll(streams, streamsList.split(","));
        }

        @Override
        protected int runCmd() throws Exception {
            final DistributedLogManagerFactory factory = new DistributedLogManagerFactory(getConf(), getUri());
            try {
                MetadataUpdater metadataUpdater = dryrun ? new DryrunZkMetadataUpdater(factory.getSharedZKClientForDL()) :
                        ZkMetadataUpdater.createMetadataUpdater(factory.getSharedZKClientForDL());
                System.out.println("List of streams : ");
                System.out.println(streams);
                if (!IOUtils.confirmPrompt("Are u sure to repair streams (Y/N):")) {
                    return -1;
                }
                for (String stream : streams) {
                    fixInprogressSegmentWithLowerSequenceNumber(factory, metadataUpdater, stream, verbose, !force);
                }
                return 0;
            } finally {
                factory.close();
            }
        }

        @Override
        protected String getUsage() {
            return "repairseqno [options]";
        }
    }

    public DistributedLogAdmin() {
        super();
        commands.clear();
        addCommand(new HelpCommand());
        addCommand(new BindCommand());
        addCommand(new UnbindCommand());
        addCommand(new RepairSeqNoCommand());
    }

    @Override
    protected String getName() {
        return "dlog_admin";
    }
}
