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
package com.twitter.distributedlog.admin;

import com.google.common.base.Preconditions;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LedgerHandleCache;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.ReadUtils;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.acl.ZKAccessControl;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.impl.federated.FederatedZKLogMetadataStore;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.DLMetadata;
import com.twitter.distributedlog.metadata.DryrunLogSegmentMetadataStoreUpdater;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import com.twitter.distributedlog.thrift.AccessControlEntry;
import com.twitter.distributedlog.tools.DistributedLogTool;
import com.twitter.distributedlog.util.DLUtils;
import com.twitter.distributedlog.util.FutureUtils;
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
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

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
@SuppressWarnings("deprecation")
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
    public static void fixInprogressSegmentWithLowerSequenceNumber(final com.twitter.distributedlog.DistributedLogManagerFactory factory,
                                                                   final MetadataUpdater metadataUpdater,
                                                                   final String streamName,
                                                                   final boolean verbose,
                                                                   final boolean interactive) throws IOException {
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        try {
            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            if (verbose) {
                System.out.println("LogSegments for " + streamName + " : ");
                for (LogSegmentMetadata segment : segments) {
                    System.out.println(segment.getLogSegmentSequenceNumber() + "\t: " + segment);
                }
            }
            LOG.info("Get log segments for {} : {}", streamName, segments);
            // validate log segments
            long maxCompletedLogSegmentSequenceNumber = -1L;
            LogSegmentMetadata inprogressSegment = null;
            for (LogSegmentMetadata segment : segments) {
                if (!segment.isInProgress()) {
                    maxCompletedLogSegmentSequenceNumber = Math.max(maxCompletedLogSegmentSequenceNumber, segment.getLogSegmentSequenceNumber());
                } else {
                    // we already found an inprogress segment
                    if (null != inprogressSegment) {
                        throw new DLIllegalStateException("Multiple inprogress segments found for stream " + streamName + " : " + segments);
                    }
                    inprogressSegment = segment;
                }
            }
            if (null == inprogressSegment || inprogressSegment.getLogSegmentSequenceNumber() > maxCompletedLogSegmentSequenceNumber) {
                // nothing to fix
                return;
            }
            final long newLogSegmentSequenceNumber = maxCompletedLogSegmentSequenceNumber + 1;
            if (interactive && !IOUtils.confirmPrompt("Confirm to fix (Y/N), Ctrl+C to break : ")) {
                return;
            }
            final LogSegmentMetadata newSegment =
                    FutureUtils.result(metadataUpdater.changeSequenceNumber(inprogressSegment, newLogSegmentSequenceNumber));
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
        final LogSegmentMetadata metadata;
        final LogRecordWithDLSN lastRecord;

        LogSegmentCandidate(LogSegmentMetadata metadata, LogRecordWithDLSN lastRecord) {
            this.metadata = metadata;
            this.lastRecord = lastRecord;
        }

        @Override
        public String toString() {
            return "LogSegmentCandidate[ metadata = " + metadata + ", last record = " + lastRecord + " ]";
        }

    }

    private static final Comparator<LogSegmentCandidate> LOG_SEGMENT_CANDIDATE_COMPARATOR =
            new Comparator<LogSegmentCandidate>() {
                @Override
                public int compare(LogSegmentCandidate o1, LogSegmentCandidate o2) {
                    return LogSegmentMetadata.COMPARATOR.compare(o1.metadata, o2.metadata);
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

        @Override
        public String toString() {
            return "StreamCandidate[ name = " + streamName + ", segments = " + segmentCandidates + " ]";
        }
    }

    public static void checkAndRepairDLNamespace(final URI uri,
                                                 final com.twitter.distributedlog.DistributedLogManagerFactory factory,
                                                 final MetadataUpdater metadataUpdater,
                                                 final ExecutorService executorService,
                                                 final BookKeeperClient bkc,
                                                 final String digestpw,
                                                 final boolean verbose,
                                                 final boolean interactive) throws IOException {
        checkAndRepairDLNamespace(uri, factory, metadataUpdater, executorService, bkc, digestpw, verbose, interactive, 1);
    }

    public static void checkAndRepairDLNamespace(final URI uri,
                                                 final com.twitter.distributedlog.DistributedLogManagerFactory factory,
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
        if (interactive && !IOUtils.confirmPrompt("Do you want to fix all " + streamCandidates.size() + " corrupted streams (Y/N) : ")) {
            return;
        }
        if (verbose) {
            System.out.println("- 1. repairing " + streamCandidates.size() + " corrupted streams.");
        }
        for (StreamCandidate candidate : streamCandidates.values()) {
            if (!repairStream(metadataUpdater, candidate, verbose, interactive)) {
                if (verbose) {
                    System.out.println("* 1. aborted repairing corrupted streams.");
                }
                return;
            }
        }
        if (verbose) {
            System.out.println("+ 1. repaired " + streamCandidates.size() + " corrupted streams.");
        }
    }

    private static Map<String, StreamCandidate> checkStreams(
            final com.twitter.distributedlog.DistributedLogManagerFactory factory,
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
                        LOG.info("Checked stream {} - {}.", stream, candidate);
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
            final com.twitter.distributedlog.DistributedLogManagerFactory factory,
            final String streamName,
            final ExecutorService executorService,
            final BookKeeperClient bkc,
            String digestpw) throws IOException {
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        try {
            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            if (segments.isEmpty()) {
                return null;
            }
            List<Future<LogSegmentCandidate>> futures =
                    new ArrayList<Future<LogSegmentCandidate>>(segments.size());
            for (LogSegmentMetadata segment : segments) {
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
            final LogSegmentMetadata metadata,
            final ExecutorService executorService,
            final BookKeeperClient bkc,
            final String digestpw) {
        if (metadata.isInProgress()) {
            return Future.value(null);
        }

        final LedgerHandleCache handleCache = LedgerHandleCache.newBuilder()
                .bkc(bkc)
                .conf(new DistributedLogConfiguration().setBKDigestPW(digestpw))
                .build();
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
                handleCache
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
        }).ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCache.clear();
                return BoxedUnit.UNIT;
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
                System.out.println("  " + segmentCandidate.metadata.getLogSegmentSequenceNumber()
                        + " : metadata = " + segmentCandidate.metadata + ", last dlsn = "
                        + segmentCandidate.lastRecord.getDlsn());
            }
            System.out.println("-------------------------------------------");
        }
        if (interactive && !IOUtils.confirmPrompt("Do you want to fix the stream " + streamCandidate.streamName + " (Y/N) : ")) {
            return false;
        }
        for (LogSegmentCandidate segmentCandidate : streamCandidate.segmentCandidates) {
            LogSegmentMetadata newMetadata = FutureUtils.result(
                    metadataUpdater.updateLastRecord(segmentCandidate.metadata, segmentCandidate.lastRecord));
            if (verbose) {
                System.out.println("  Fixed segment " + segmentCandidate.metadata.getLogSegmentSequenceNumber() + " : ");
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
                System.err.println("No distributedlog uri specified.");
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
                System.out.println("No bookkeeper is bound to " + uri);
                return 0;
            } else {
                System.out.println("There is bookkeeper bound to " + uri + " : ");
                System.out.println("");
                System.out.println(bkdlConfig.toString());
                System.out.println("");
                if (!force && !IOUtils.confirmPrompt("Do you want to unbind " + uri + " :\n")) {
                    return 0;
                }
            }
            DLMetadata.unbind(uri);
            System.out.println("Unbound on " + uri + ".");
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
            options.addOption("seqno", "firstLogSegmentSeqNo", true, "The first log segment sequence number to use after upgrade");
            options.addOption("fns", "federatedNamespace", false, "Flag to turn a namespace to federated namespace");
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
                System.err.println("Error: Neither zkServers nor ledgersPath specified for bookkeeper environment.");
                printUsage();
                return -1;
            }
            String[] args = cmdline.getArgs();
            if (args.length <= 0) {
                System.err.println("No distributedlog uri specified.");
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

                if (cmdline.hasOption("seqno")) {
                    newBKDLConfig = newBKDLConfig.setFirstLogSegmentSeqNo(Long.parseLong(cmdline.getOptionValue("seqno")));
                }

                if (cmdline.hasOption("fns")) {
                    newBKDLConfig = newBKDLConfig.setFederatedNamespace(true);
                }

                BKDLConfig bkdlConfig;
                try {
                    bkdlConfig = BKDLConfig.resolveDLConfig(zkc, uri);
                } catch (IOException ie) {
                    bkdlConfig = null;
                }
                if (null == bkdlConfig) {
                    System.out.println("No bookkeeper is bound to " + uri);
                } else {
                    System.out.println("There is bookkeeper bound to " + uri + " : ");
                    System.out.println("");
                    System.out.println(bkdlConfig.toString());
                    System.out.println("");
                    if (!isQuery) {
                        if (newBKDLConfig.equals(bkdlConfig)) {
                            System.out.println("No bookkeeper binding needs to be updated. Quit.");
                            return 0;
                        } else if(!newBKDLConfig.isFederatedNamespace() && bkdlConfig.isFederatedNamespace()) {
                            System.out.println("You can't turn a federated namespace back to non-federated.");
                            return 0;
                        } else {
                            if (!force && !IOUtils.confirmPrompt("Do you want to bind " + uri
                                        + " with new bookkeeper instance :\n" + newBKDLConfig)) {
                                return 0;
                            }
                        }
                    }
                }
                if (isQuery) {
                    System.out.println("Done.");
                    return 0;
                }
                DLMetadata dlMetadata = DLMetadata.create(newBKDLConfig);
                if (creation) {
                    try {
                        dlMetadata.create(uri);
                        System.out.println("Created binding on " + uri + ".");
                    } catch (IOException ie) {
                        System.err.println("Failed to create binding on " + uri + " : " + ie.getMessage());
                    }
                } else {
                    try {
                        dlMetadata.update(uri);
                        System.out.println("Updated binding on " + uri + " : ");
                        System.out.println("");
                        System.out.println(newBKDLConfig.toString());
                        System.out.println("");
                    } catch (IOException ie) {
                        System.err.println("Failed to update binding on " + uri + " : " + ie.getMessage());
                    }
                }
                if (newBKDLConfig.isFederatedNamespace()) {
                    try {
                        FederatedZKLogMetadataStore.createFederatedNamespace(uri, zkc);
                    } catch (KeeperException.NodeExistsException nee) {
                        // ignore node exists exception
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
            MetadataUpdater metadataUpdater = dryrun ?
                    new DryrunLogSegmentMetadataStoreUpdater(getConf(),
                            getLogSegmentMetadataStore()) :
                    LogSegmentMetadataStoreUpdater.createMetadataUpdater(getConf(),
                            getLogSegmentMetadataStore());
            System.out.println("List of streams : ");
            System.out.println(streams);
            if (!IOUtils.confirmPrompt("Do you want to repair all these streams (Y/N):")) {
                return -1;
            }
            for (String stream : streams) {
                fixInprogressSegmentWithLowerSequenceNumber(getFactory(), metadataUpdater, stream, verbose, !getForce());
            }
            return 0;
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
            MetadataUpdater metadataUpdater = dryrun ?
                    new DryrunLogSegmentMetadataStoreUpdater(getConf(),
                            getLogSegmentMetadataStore()) :
                    LogSegmentMetadataStoreUpdater.createMetadataUpdater(getConf(),
                            getLogSegmentMetadataStore());
            ExecutorService executorService = Executors.newCachedThreadPool();
            BookKeeperClient bkc = getBookKeeperClient();
            try {
                checkAndRepairDLNamespace(getUri(), getFactory(), metadataUpdater, executorService,
                                          bkc, getConf().getBKDigestPW(), verbose, !getForce(), concurrency);
            } finally {
                SchedulerUtils.shutdownScheduler(executorService, 5, TimeUnit.MINUTES);
            }
            return 0;
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
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(getZooKeeperClient(), getUri());
            if (null == bkdlConfig.getACLRootPath()) {
                // acl isn't enabled for this namespace.
                System.err.println("ACL isn't enabled for namespace " + getUri());
                return -1;
            }
            String zkPath = getUri() + "/" + bkdlConfig.getACLRootPath() + "/" + stream;
            ZKAccessControl.delete(getZooKeeperClient(), zkPath);
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
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(getZooKeeperClient(), getUri());
            if (null == bkdlConfig.getACLRootPath()) {
                // acl isn't enabled for this namespace.
                System.err.println("ACL isn't enabled for namespace " + getUri());
                return -1;
            }
            String zkPath = getZKPath(getUri().getPath() + "/" + bkdlConfig.getACLRootPath());
            ZKAccessControl accessControl = getZKAccessControl(getZooKeeperClient(), zkPath);
            AccessControlEntry acl = accessControl.getAccessControlEntry();
            acl.setDenyWrite(denyWrite);
            acl.setDenyTruncate(denyTruncate);
            acl.setDenyDelete(denyDelete);
            acl.setDenyAcquire(denyAcquire);
            acl.setDenyRelease(denyRelease);
            setZKAccessControl(getZooKeeperClient(), accessControl);
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
