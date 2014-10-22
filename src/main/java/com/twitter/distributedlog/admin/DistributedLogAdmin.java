package com.twitter.distributedlog.admin;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.ReadUtils;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.acl.ZKAccessControl;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.twitter.distributedlog.metadata.DryrunZkMetadataUpdater;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import com.twitter.distributedlog.thrift.AccessControlEntry;
import com.twitter.distributedlog.tools.DistributedLogTool;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static class LogSegmentCandidate {
        final LogSegmentLedgerMetadata metadata;
        final LogRecordWithDLSN lastRecord;

        LogSegmentCandidate(LogSegmentLedgerMetadata metadata, LogRecordWithDLSN lastRecord) {
            this.metadata = metadata;
            this.lastRecord = lastRecord;
        }
    }

    private static final Comparator<LogSegmentCandidate> LOG_SEGMENT_CANDIDATE_COMPARATOR =
            new Comparator<LogSegmentCandidate>() {
                @Override
                public int compare(LogSegmentCandidate o1, LogSegmentCandidate o2) {
                    return LogSegmentLedgerMetadata.COMPARATOR.compare(o1.metadata, o2.metadata);
                }
            };

    private static class StreamCandidate {

        final String streamName;
        final SortedSet<LogSegmentCandidate> segmentCandidates =
                new TreeSet<LogSegmentCandidate>(LOG_SEGMENT_CANDIDATE_COMPARATOR);

        StreamCandidate(String streamName) {
            this.streamName = streamName;
        }

        synchronized void addLogSegmentCandidate(LogSegmentCandidate segmentCandidate) {
            segmentCandidates.add(segmentCandidate);
        }
    }

    public static void checkAndRepairDLNamespace(final URI uri,
                                                 final DistributedLogManagerFactory factory,
                                                 final MetadataUpdater metadataUpdater,
                                                 final ExecutorService executorService,
                                                 final BookKeeperClient bkc,
                                                 final String digestpw,
                                                 final boolean verbose,
                                                 final boolean interactive) throws IOException {
        checkAndRepairDLNamespace(uri, factory, metadataUpdater, executorService, bkc, digestpw, verbose, interactive, 1);
    }

    public static void checkAndRepairDLNamespace(final URI uri,
                                                 final DistributedLogManagerFactory factory,
                                                 final MetadataUpdater metadataUpdater,
                                                 final ExecutorService executorService,
                                                 final BookKeeperClient bkc,
                                                 final String digestpw,
                                                 final boolean verbose,
                                                 final boolean interactive,
                                                 final int concurrency) throws IOException {
        Preconditions.checkArgument(concurrency > 0, "Invalid concurrency " + concurrency + " found.");
        // 0. getting streams under a given uri.
        Collection<String> streams = factory.enumerateAllLogsInNamespace();
        if (verbose) {
            System.out.println("- 0. checking " + streams.size() + " streams under " + uri);
        }
        if (streams.size() == 0) {
            System.out.println("+ 0. nothing to check. quit.");
            return;
        }
        Map<String, StreamCandidate> streamCandidates =
                checkStreams(factory, streams, executorService, bkc, digestpw, concurrency);
        if (verbose) {
            System.out.println("+ 0. " + streamCandidates.size() + " corrupted streams found.");
        }
        if (interactive && !IOUtils.confirmPrompt("Are u going to fix all " + streamCandidates.size() + " corrupted streams (Y/N) : ")) {
            return;
        }
        if (verbose) {
            System.out.println("- 1. repairing " + streamCandidates.size() + " corrupted streams.");
        }
        for (StreamCandidate candidate : streamCandidates.values()) {
            if (!repairStream(metadataUpdater, candidate, verbose, interactive)) {
                if (verbose) {
                    System.out.println("* 1. aborted repairing corrupted streams. byebye!");
                }
                return;
            }
        }
        if (verbose) {
            System.out.println("+ 1. repaired " + streamCandidates.size() + " corrupted streams.");
        }
    }

    private static Map<String, StreamCandidate> checkStreams(
            final DistributedLogManagerFactory factory,
            final Collection<String> streams,
            final ExecutorService executorService,
            final BookKeeperClient bkc,
            final String digestpw,
            final int concurrency) throws IOException {
        final LinkedBlockingQueue<String> streamQueue =
                new LinkedBlockingQueue<String>();
        streamQueue.addAll(streams);
        final Map<String, StreamCandidate> candidateMap =
                new ConcurrentSkipListMap<String, StreamCandidate>();
        final AtomicInteger numPendingStreams = new AtomicInteger(streams.size());
        final CountDownLatch doneLatch = new CountDownLatch(1);
        Runnable checkRunnable = new Runnable() {
            @Override
            public void run() {
                while (!streamQueue.isEmpty()) {
                    String stream;
                    try {
                        stream = streamQueue.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    StreamCandidate candidate;
                    try {
                        LOG.info("Checking stream {}.", stream);
                        candidate = checkStream(factory, stream, executorService, bkc, digestpw);
                        LOG.info("Checked stream {}.", stream);
                    } catch (IOException e) {
                        LOG.error("Error on checking stream {} : ", stream, e);
                        doneLatch.countDown();
                        break;
                    }
                    if (null != candidate) {
                        candidateMap.put(stream, candidate);
                    }
                    if (numPendingStreams.decrementAndGet() == 0) {
                        doneLatch.countDown();
                    }
                }
            }
        };
        Thread[] threads = new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            threads[i] = new Thread(checkRunnable, "check-thread-" + i);
            threads[i].start();
        }
        try {
            doneLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (numPendingStreams.get() != 0) {
            throw new IOException(numPendingStreams.get() + " streams left w/o checked");
        }
        for (int i = 0; i < concurrency; i++) {
            threads[i].interrupt();
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return candidateMap;
    }

    private static StreamCandidate checkStream(
            final DistributedLogManagerFactory factory,
            final String streamName,
            final ExecutorService executorService,
            final BookKeeperClient bkc,
            String digestpw) throws IOException {
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        try {
            List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
            if (segments.isEmpty()) {
                return null;
            }
            List<Future<LogSegmentCandidate>> futures =
                    new ArrayList<Future<LogSegmentCandidate>>(segments.size());
            for (LogSegmentLedgerMetadata segment : segments) {
                futures.add(checkLogSegment(streamName, segment, executorService, bkc, digestpw));
            }
            List<LogSegmentCandidate> segmentCandidates;
            try {
                segmentCandidates = Await.result(Future.collect(futures));
            } catch (Exception e) {
                throw new IOException("Failed on checking stream " + streamName, e);
            }
            StreamCandidate streamCandidate = new StreamCandidate(streamName);
            for (LogSegmentCandidate segmentCandidate: segmentCandidates) {
                if (null != segmentCandidate) {
                    streamCandidate.addLogSegmentCandidate(segmentCandidate);
                }
            }
            if (streamCandidate.segmentCandidates.isEmpty()) {
                return null;
            }
            return streamCandidate;
        } finally {
            dlm.close();
        }
    }

    private static Future<LogSegmentCandidate> checkLogSegment(
            final String streamName,
            final LogSegmentLedgerMetadata metadata,
            final ExecutorService executorService,
            final BookKeeperClient bkc,
            final String digestpw) {
        if (metadata.isInProgress()) {
            return Future.value(null);
        }

        return ReadUtils.asyncReadLastRecord(
                streamName,
                metadata,
                true,
                false,
                true,
                4,
                16,
                new AtomicInteger(0),
                executorService,
                bkc,
                digestpw
        ).map(new Function<LogRecordWithDLSN, LogSegmentCandidate>() {
            @Override
            public LogSegmentCandidate apply(LogRecordWithDLSN record) {
                if (null != record &&
                    (record.getDlsn().compareTo(metadata.getLastDLSN()) > 0 ||
                     record.getTransactionId() > metadata.getLastTxId() ||
                     !metadata.isRecordPositionWithinSegmentScope(record))) {
                    return new LogSegmentCandidate(metadata, record);
                } else {
                    return null;
                }
            }
        });
    }

    private static boolean repairStream(MetadataUpdater metadataUpdater,
                                        StreamCandidate streamCandidate,
                                        boolean verbose,
                                        boolean interactive) throws IOException {
        if (verbose) {
            System.out.println("Stream " + streamCandidate.streamName + " : ");
            for (LogSegmentCandidate segmentCandidate : streamCandidate.segmentCandidates) {
                System.out.println("  " + segmentCandidate.metadata.getLedgerSequenceNumber()
                        + " : metadata = " + segmentCandidate.metadata + ", last dlsn = "
                        + segmentCandidate.lastRecord.getDlsn());
            }
            System.out.println("-------------------------------------------");
        }
        if (interactive && !IOUtils.confirmPrompt("Are u sure to fix stream " + streamCandidate.streamName + " (Y/N) : ")) {
            return false;
        }
        for (LogSegmentCandidate segmentCandidate : streamCandidate.segmentCandidates) {
            LogSegmentLedgerMetadata newMetadata =
                    metadataUpdater.updateLastRecord(segmentCandidate.metadata, segmentCandidate.lastRecord);
            if (verbose) {
                System.out.println("  Fixed segment " + segmentCandidate.metadata.getLedgerSequenceNumber() + " : ");
                System.out.println("    old metadata : " + segmentCandidate.metadata);
                System.out.println("    new metadata : " + newMetadata);
            }
        }
        if (verbose) {
            System.out.println("-------------------------------------------");
        }
        return true;
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
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder().uri(uri).zkAclId(null)
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

    static class RepairSeqNoCommand extends PerDLCommand {

        boolean dryrun = false;
        boolean verbose = false;
        final List<String> streams = new ArrayList<String>();

        RepairSeqNoCommand() {
            super("repairseqno", "Repair a stream whose inprogress log segment has lower sequence number.");
            options.addOption("d", "dryrun", false, "Dry run without repairing");
            options.addOption("l", "list", true, "List of streams to repair, separated by comma");
            options.addOption("v", "verbose", false, "Print verbose messages");
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
                MetadataUpdater metadataUpdater = dryrun ? new DryrunZkMetadataUpdater(factory.getSharedWriterZKCForDL()) :
                        ZkMetadataUpdater.createMetadataUpdater(factory.getSharedWriterZKCForDL());
                System.out.println("List of streams : ");
                System.out.println(streams);
                if (!IOUtils.confirmPrompt("Are u sure to repair streams (Y/N):")) {
                    return -1;
                }
                for (String stream : streams) {
                    fixInprogressSegmentWithLowerSequenceNumber(factory, metadataUpdater, stream, verbose, !getForce());
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

    static class DLCKCommand extends PerDLCommand {

        boolean dryrun = false;
        boolean verbose = false;
        int concurrency = 1;

        DLCKCommand() {
            super("dlck", "Check and repair a distributedlog namespace");
            options.addOption("d", "dryrun", false, "Dry run without repairing");
            options.addOption("v", "verbose", false, "Print verbose messages");
            options.addOption("cy", "concurrency", true, "Concurrency on checking streams");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            dryrun = cmdline.hasOption("d");
            verbose = cmdline.hasOption("v");
            if (cmdline.hasOption("cy")) {
                try {
                    concurrency = Integer.parseInt(cmdline.getOptionValue("cy"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid concurrency value : " + cmdline.getOptionValue("cy"));
                }
            }
        }

        @Override
        protected int runCmd() throws Exception {
            final DistributedLogManagerFactory factory = new DistributedLogManagerFactory(getConf(), getUri());
            try {
                MetadataUpdater metadataUpdater = dryrun ? new DryrunZkMetadataUpdater(factory.getSharedWriterZKCForDL()) :
                        ZkMetadataUpdater.createMetadataUpdater(factory.getSharedWriterZKCForDL());
                ExecutorService executorService = Executors.newCachedThreadPool();
                BookKeeperClient bkc = factory.getReaderBKC();
                try {
                    checkAndRepairDLNamespace(getUri(), factory, metadataUpdater, executorService,
                                              bkc, getConf().getBKDigestPW(), verbose, !getForce(), concurrency);
                } finally {
                    SchedulerUtils.shutdownScheduler(executorService, 5, TimeUnit.MINUTES);
                }
                return 0;
            } finally {
                factory.close();
            }
        }

        @Override
        protected String getUsage() {
            return "dlck [options]";
        }
    }

    static class DeleteStreamACLCommand extends PerDLCommand {

        String stream = null;

        DeleteStreamACLCommand() {
            super("delete_stream_acl", "Delete ACL for a given stream");
            options.addOption("s", "stream", true, "Stream to set ACL");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("s")) {
                throw new ParseException("No stream to set ACL");
            }
            stream = cmdline.getOptionValue("s");
        }

        @Override
        protected int runCmd() throws Exception {
            final DistributedLogManagerFactory factory = new DistributedLogManagerFactory(getConf(), getUri());
            try {
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(factory.getSharedWriterZKCForDL(), getUri());
                if (null == bkdlConfig.getACLRootPath()) {
                    // acl isn't enabled for this namespace.
                    println("ACL isn't enabled for namespace " + getUri());
                    return -1;
                }
                String zkPath = getUri() + "/" + bkdlConfig.getACLRootPath() + "/" + stream;
                ZKAccessControl.delete(factory.getSharedWriterZKCForDL(), zkPath);
            } finally {
                factory.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return null;
        }
    }

    static class SetStreamACLCommand extends SetACLCommand {

        String stream = null;

        SetStreamACLCommand() {
            super("set_stream_acl", "Set Default ACL for a given stream");
            options.addOption("s", "stream", true, "Stream to set ACL");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("s")) {
                throw new ParseException("No stream to set ACL");
            }
            stream = cmdline.getOptionValue("s");
        }

        @Override
        protected String getZKPath(String zkRootPath) {
            return zkRootPath + "/" + stream;
        }

        @Override
        protected String getUsage() {
            return "set_stream_acl [options]";
        }
    }

    static class SetDefaultACLCommand extends SetACLCommand {

        SetDefaultACLCommand() {
            super("set_default_acl", "Set Default ACL for a namespace");
        }

        @Override
        protected String getZKPath(String zkRootPath) {
            return zkRootPath;
        }

        @Override
        protected String getUsage() {
            return "set_default_acl [options]";
        }
    }

    static abstract class SetACLCommand extends PerDLCommand {

        boolean denyWrite = false;
        boolean denyTruncate = false;
        boolean denyDelete = false;
        boolean denyAcquire = false;
        boolean denyRelease = false;

        protected SetACLCommand(String name, String description) {
            super(name, description);
            options.addOption("dw", "deny-write", false, "Deny write/bulkWrite requests");
            options.addOption("dt", "deny-truncate", false, "Deny truncate requests");
            options.addOption("dd", "deny-delete", false, "Deny delete requests");
            options.addOption("da", "deny-acquire", false, "Deny acquire requests");
            options.addOption("dr", "deny-release", false, "Deny release requests");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            denyWrite = cmdline.hasOption("dw");
            denyTruncate = cmdline.hasOption("dt");
            denyDelete = cmdline.hasOption("dd");
            denyAcquire = cmdline.hasOption("da");
            denyRelease = cmdline.hasOption("dr");
        }

        protected abstract String getZKPath(String zkRootPath);

        protected ZKAccessControl getZKAccessControl(ZooKeeperClient zkc, String zkPath) throws Exception {
            ZKAccessControl accessControl;
            try {
                accessControl = Await.result(ZKAccessControl.read(zkc, zkPath, null));
            } catch (KeeperException.NoNodeException nne) {
                accessControl = new ZKAccessControl(new AccessControlEntry(), zkPath);
            }
            return accessControl;
        }

        protected void setZKAccessControl(ZooKeeperClient zkc, ZKAccessControl accessControl) throws Exception {
            String zkPath = accessControl.getZKPath();
            if (null == zkc.get().exists(zkPath, false)) {
                accessControl.create(zkc);
            } else {
                accessControl.update(zkc);
            }
        }

        @Override
        protected int runCmd() throws Exception {
            final DistributedLogManagerFactory factory = new DistributedLogManagerFactory(getConf(), getUri());
            try {
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(factory.getSharedWriterZKCForDL(), getUri());
                if (null == bkdlConfig.getACLRootPath()) {
                    // acl isn't enabled for this namespace.
                    println("ACL isn't enabled for namespace " + getUri());
                    return -1;
                }
                String zkPath = getZKPath(getUri().getPath() + "/" + bkdlConfig.getACLRootPath());
                ZKAccessControl accessControl = getZKAccessControl(factory.getSharedWriterZKCForDL(), zkPath);
                AccessControlEntry acl = accessControl.getAccessControlEntry();
                acl.setDenyWrite(denyWrite);
                acl.setDenyTruncate(denyTruncate);
                acl.setDenyDelete(denyDelete);
                acl.setDenyAcquire(denyAcquire);
                acl.setDenyRelease(denyRelease);
                setZKAccessControl(factory.getSharedWriterZKCForDL(), accessControl);
            } finally {
                factory.close();
            }
            return 0;
        }

    }

    public DistributedLogAdmin() {
        super();
        commands.clear();
        addCommand(new HelpCommand());
        addCommand(new BindCommand());
        addCommand(new UnbindCommand());
        addCommand(new RepairSeqNoCommand());
        addCommand(new DLCKCommand());
        addCommand(new SetDefaultACLCommand());
        addCommand(new SetStreamACLCommand());
        addCommand(new DeleteStreamACLCommand());
    }

    @Override
    protected String getName() {
        return "dlog_admin";
    }
}
