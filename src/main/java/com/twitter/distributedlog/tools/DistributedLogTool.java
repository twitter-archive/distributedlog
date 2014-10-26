package com.twitter.distributedlog.tools;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LedgerEntryReader;
import com.twitter.distributedlog.LogEmptyException;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.PartitionId;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.LedgerAllocatorUtils;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerReader;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;

public class DistributedLogTool extends Tool {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogTool.class);

    static final List<String> EMPTY_LIST = Lists.newArrayList();

    static int compareByCompletionTime(long time1, long time2) {
        return time1 > time2 ? 1 : (time1 < time2 ? -1 : 0);
    }

    static final Comparator<LogSegmentLedgerMetadata> LOGSEGMENT_COMPARATOR_BY_TIME = new Comparator<LogSegmentLedgerMetadata>() {
        @Override
        public int compare(LogSegmentLedgerMetadata o1, LogSegmentLedgerMetadata o2) {
            if (o1.isInProgress() && o2.isInProgress()) {
                return compareByCompletionTime(o1.getFirstTxId(), o2.getFirstTxId());
            } else if (!o1.isInProgress() && !o2.isInProgress()) {
                return compareByCompletionTime(o1.getCompletionTime(), o2.getCompletionTime());
            } else if (o1.isInProgress() && !o2.isInProgress()) {
                return compareByCompletionTime(o1.getFirstTxId(), o2.getCompletionTime());
            } else {
                return compareByCompletionTime(o1.getCompletionTime(), o2.getFirstTxId());
            }
        }
    };

    static DLSN parseDLSN(String dlsnStr) throws IOException {
        String[] parts = dlsnStr.split(",");
        if (parts.length != 3) {
            throw new IOException("Invalid dlsn : " + dlsnStr);
        }
        try {
            return new DLSN(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]));
        } catch (NumberFormatException nfe) {
            throw new IOException("Invalid dlsn : " + dlsnStr, nfe);
        }
    }

    /**
     * Per DL Command, which parses basic options. e.g. uri.
     */
    protected abstract static class PerDLCommand extends OptsCommand {

        protected Options options = new Options();
        protected final DistributedLogConfiguration dlConf;
        protected URI uri;
        protected String zkAclId = null;
        protected boolean force = false;

        protected PerDLCommand(String name, String description) {
            super(name, description);
            dlConf = new DistributedLogConfiguration();
            options.addOption("u", "uri", true, "DistributedLog URI");
            options.addOption("c", "conf", true, "DistributedLog Configuration File");
            options.addOption("a", "zk-acl-id", true, "Zookeeper ACL ID");
            options.addOption("f", "force", false, "Force command (no warnings or prompts)");
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
            return runCmd();
        }

        protected abstract int runCmd() throws Exception;

        @Override
        protected Options getOptions() {
            return options;
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            if (!cmdline.hasOption("u")) {
                throw new ParseException("No distributedlog uri provided.");
            }
            uri = URI.create(cmdline.getOptionValue("u"));
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
            if (cmdline.hasOption("a")) {
                zkAclId = cmdline.getOptionValue("a");
            }
            if (cmdline.hasOption("f")) {
                force = true;
            }
        }

        protected DistributedLogConfiguration getConf() {
            return dlConf;
        }

        protected URI getUri() {
            return uri;
        }

        protected void setUri(URI uri) {
            this.uri = uri;
        }

        protected String getZkAclId() {
            return zkAclId;
        }

        protected void setZkAclId(String zkAclId) {
            this.zkAclId = zkAclId;
        }

        protected boolean getForce() {
            return force;
        }

        protected void setForce(boolean force) {
            this.force = force;
        }
    }

    /**
     * Per Stream Command, which parse common options for per stream. e.g. stream name.
     */
    abstract static class PerStreamCommand extends PerDLCommand {

        protected String streamName;

        protected PerStreamCommand(String name, String description) {
            super(name, description);
            options.addOption("s", "stream", true, "Stream Name");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("s")) {
                throw new ParseException("No stream name provided.");
            }
            streamName = cmdline.getOptionValue("s");
        }

        protected String getStreamName() {
            return streamName;
        }

        protected void setStreamName(String streamName) {
            this.streamName = streamName;
        }
    }

    protected static class DeleteAllocatorPoolCommand extends PerDLCommand {

        DeleteAllocatorPoolCommand() {
            super("delete_allocator_pool", "Delete allocator pool for a given distributedlog instance");
        }

        @Override
        protected int runCmd() throws Exception {
            String rootPath = getUri().getPath() + "/" + DistributedLogConstants.ALLOCATION_POOL_NODE;
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                    .sessionTimeoutMs(dlConf.getZKSessionTimeoutMilliseconds())
                    .uri(getUri()).zkAclId(getZkAclId()).build();
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, getUri());
            BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                    .dlConfig(getConf())
                    .zkServers(bkdlConfig.getBkZkServersForWriter())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .name("dlog_tool")
                    .build();
            try {
                List<String> pools = zkc.get().getChildren(rootPath, false);
                if (getForce() || IOUtils.confirmPrompt("Are you sure you want to delete allocator pools : " + pools)) {
                    for (String pool : pools) {
                        String poolPath = rootPath + "/" + pool;
                        LedgerAllocator allocator =
                                LedgerAllocatorUtils.createLedgerAllocatorPool(poolPath, 0, getConf(), zkc, bkc);
                        if (null == allocator) {
                            println("ERROR: use zk34 version to delete allocator pool : " + poolPath + " .");
                        } else {
                            allocator.delete();
                            println("Deleted allocator pool : " + poolPath + " .");
                        }
                    }
                }
            } finally {
                bkc.close();
                zkc.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "delete_allocator_pool";
        }
    }

    public static class ListCommand extends PerDLCommand {

        boolean printMetadata = false;
        boolean printHex = false;

        ListCommand() {
            super("list", "list streams of a given distributedlog instance");
            options.addOption("m", "meta", false, "Print metadata associated with each stream");
            options.addOption("x", "hex", false, "Print metadata in hex format");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            printMetadata = cmdline.hasOption("m");
            printHex = cmdline.hasOption("x");
        }

        @Override
        protected String getUsage() {
            return "list [options]";
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManagerFactory factory =
                    new DistributedLogManagerFactory(getConf(), getUri());
            try {
                if (printMetadata) {
                    printStreamsWithMetadata(factory);
                } else {
                    printStreams(factory);
                }
            } finally {
                factory.close();
            }
            return 0;
        }

        protected void printStreamsWithMetadata(DistributedLogManagerFactory factory)
                throws Exception {
            Map<String, byte[]> streams = factory.enumerateLogsWithMetadataInNamespace();
            println("Stream under " + getUri() + " : ");
            println("--------------------------------");
            for (Map.Entry<String, byte[]> entry : streams.entrySet()) {
                println(entry.getKey());
                if (null == entry.getValue() || entry.getValue().length == 0) {
                    continue;
                }
                if (printHex) {
                    println(Hex.encodeHexString(entry.getValue()));
                } else {
                    println(new String(entry.getValue(), UTF_8));
                }
                println("");
            }
            println("--------------------------------");
        }

        protected void printStreams(DistributedLogManagerFactory factory) throws Exception {
            Collection<String> streams = factory.enumerateAllLogsInNamespace();
            println("Steams under " + getUri() + " : ");
            println("--------------------------------");
            for (String stream : streams) {
                println(stream);
            }
            println("--------------------------------");
        }
    }

    protected static class InspectCommand extends PerDLCommand {

        int numThreads = 1;
        String streamPrefix = null;
        boolean printInprogressOnly = false;
        boolean dumpEntries = false;
        boolean orderByTime = false;
        boolean printStreamsOnly = false;

        InspectCommand() {
            super("inspect", "Inspect streams under a given dl uri to find any potential corruptions");
            options.addOption("t", "threads", true, "Number threads to do inspection.");
            options.addOption("ft", "filter", true, "Stream filter by prefix");
            options.addOption("i", "inprogress", false, "Print inprogress log segments only");
            options.addOption("d", "dump", false, "Dump entries of inprogress log segments");
            options.addOption("ot", "orderbytime", false, "Order the log segments by completion time");
            options.addOption("pso", "print-stream-only", false, "Print streams only");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("t")) {
                numThreads = Integer.parseInt(cmdline.getOptionValue("t"));
            }
            if (cmdline.hasOption("ft")) {
                streamPrefix = cmdline.getOptionValue("ft");
            }
            printInprogressOnly = cmdline.hasOption("i");
            dumpEntries = cmdline.hasOption("d");
            orderByTime = cmdline.hasOption("ot");
            printStreamsOnly = cmdline.hasOption("pso");
        }

        @Override
        protected int runCmd() throws Exception {
            final DistributedLogManagerFactory factory =
                    new DistributedLogManagerFactory(getConf(), getUri());
            try {
                SortedMap<String, List<Pair<LogSegmentLedgerMetadata, List<String>>>> corruptedCandidates =
                        new TreeMap<String, List<Pair<LogSegmentLedgerMetadata, List<String>>>>();
                inspectStreams(factory, corruptedCandidates);
                System.out.println("Corrupted Candidates : ");
                if (printStreamsOnly) {
                    System.out.println(corruptedCandidates.keySet());
                    return 0;
                }
                for (Map.Entry<String, List<Pair<LogSegmentLedgerMetadata, List<String>>>> entry : corruptedCandidates.entrySet()) {
                    System.out.println(entry.getKey() + " : \n");
                    List<LogSegmentLedgerMetadata> segments = new ArrayList<LogSegmentLedgerMetadata>(entry.getValue().size());
                    for (Pair<LogSegmentLedgerMetadata, List<String>> pair : entry.getValue()) {
                        segments.add(pair.getLeft());
                        System.out.println("\t - " + pair.getLeft());
                        if (printInprogressOnly && dumpEntries) {
                            int i = 0;
                            for (String entryData : pair.getRight()) {
                                System.out.println("\t" + i + "\t: " + entryData);
                                ++i;
                            }
                        }
                    }
                    System.out.println();
                }
                return 0;
            } finally {
                factory.close();
            }
        }

        private void inspectStreams(final DistributedLogManagerFactory factory,
                                    final SortedMap<String, List<Pair<LogSegmentLedgerMetadata, List<String>>>> corruptedCandidates)
                throws Exception {
            Collection<String> streamCollection = factory.enumerateAllLogsInNamespace();
            final List<String> streams = new ArrayList<String>();
            if (null != streamPrefix) {
                for (String s : streamCollection) {
                    if (s.startsWith(streamPrefix)) {
                        streams.add(s);
                    }
                }
            } else {
                streams.addAll(streamCollection);
            }
            if (0 == streams.size()) {
                return;
            }
            println("Streams : " + streams);
            if (!getForce() && !IOUtils.confirmPrompt("Are you sure you want to inspect " + streams.size() + " streams")) {
                return;
            }
            numThreads = Math.min(streams.size(), numThreads);
            final int numStreamsPerThreads = streams.size() / numThreads;
            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final int tid = i;
                threads[i] = new Thread("Inspect-" + i) {
                    @Override
                    public void run() {
                        try {
                            inspectStreams(factory, streams, tid, numStreamsPerThreads, corruptedCandidates);
                            println("Thread " + tid + " finished.");
                        } catch (Exception e) {
                            println("Thread " + tid + " quits with exception : " + e.getMessage());
                        }
                    }
                };
                threads[i].start();
            }
            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }
        }

        private void inspectStreams(DistributedLogManagerFactory factory, List<String> streams,
                                    int tid, int numStreamsPerThreads,
                                    SortedMap<String, List<Pair<LogSegmentLedgerMetadata, List<String>>>> corruptedCandidates) throws Exception {
            int startIdx = tid * numStreamsPerThreads;
            int endIdx = Math.min(streams.size(), (tid + 1) * numStreamsPerThreads);
            for (int i = startIdx; i < endIdx; i++) {
                String s = streams.get(i);
                BookKeeperClient bkc = factory.getReaderBKC();
                DistributedLogManager dlm =
                        factory.createDistributedLogManagerWithSharedClients(s);
                try {
                    List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
                    if (segments.size() <= 1) {
                        continue;
                    }
                    LogSegmentLedgerMetadata firstSegment = segments.get(0);
                    long lastSeqNo = firstSegment.getLedgerSequenceNumber();
                    boolean isCandidate = false;
                    for (int j = 1; j < segments.size(); j++) {
                        LogSegmentLedgerMetadata nextSegment = segments.get(j);
                        if (lastSeqNo + 1 != nextSegment.getLedgerSequenceNumber()) {
                            isCandidate = true;
                            break;
                        }
                        ++lastSeqNo;
                    }
                    if (isCandidate) {
                        if (orderByTime) {
                            Collections.sort(segments, LOGSEGMENT_COMPARATOR_BY_TIME);
                        }
                        List<Pair<LogSegmentLedgerMetadata, List<String>>> ledgers =
                                new ArrayList<Pair<LogSegmentLedgerMetadata, List<String>>>();
                        for (LogSegmentLedgerMetadata seg : segments) {
                            LogSegmentLedgerMetadata segment = seg;
                            List<String> dumpedEntries = new ArrayList<String>();
                            if (segment.isInProgress()) {
                                LedgerHandle lh = bkc.get().openLedgerNoRecovery(segment.getLedgerId(), BookKeeper.DigestType.CRC32,
                                                                                 dlConf.getBKDigestPW().getBytes(UTF_8));
                                try {
                                    long lac = lh.readLastConfirmed();
                                    segment = segment.mutator().setLastEntryId(lac).build();
                                    if (printInprogressOnly && dumpEntries && lac >= 0) {
                                        Enumeration<LedgerEntry> entries = lh.readEntries(0L, lac);
                                        while (entries.hasMoreElements()) {
                                            LedgerEntry entry = entries.nextElement();
                                            dumpedEntries.add(new String(entry.getEntry(), UTF_8));
                                        }
                                    }
                                } finally {
                                    lh.close();
                                }
                            }
                            if (printInprogressOnly) {
                                if (segment.isInProgress()) {
                                    ledgers.add(Pair.of(segment, dumpedEntries));
                                }
                            } else {
                                ledgers.add(Pair.of(segment, EMPTY_LIST));
                            }
                        }
                        synchronized (corruptedCandidates) {
                            corruptedCandidates.put(s, ledgers);
                        }
                    }
                } finally {
                    dlm.close();
                }
            }
        }

        @Override
        protected String getUsage() {
            return "inspect [options]";
        }
    }

    protected static class TruncateCommand extends PerDLCommand {

        int numThreads = 1;
        String streamPrefix = null;
        boolean deleteStream = false;

        TruncateCommand() {
            super("truncate", "truncate streams under a given dl uri");
            options.addOption("t", "threads", true, "Number threads to do truncation");
            options.addOption("ft", "filter", true, "Stream filter by prefix");
            options.addOption("d", "delete", false, "Delete Stream");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("t")) {
                numThreads = Integer.parseInt(cmdline.getOptionValue("t"));
            }
            if (cmdline.hasOption("ft")) {
                streamPrefix = cmdline.getOptionValue("ft");
            }
            if (cmdline.hasOption("d")) {
                deleteStream = true;
            }
        }

        @Override
        protected String getUsage() {
            return "truncate [options]";
        }

        protected void setFilter(String filter) {
            this.streamPrefix = filter;
        }

        @Override
        protected int runCmd() throws Exception {
            getConf().setZkAclId(getZkAclId());
            final DistributedLogManagerFactory factory =
                    new DistributedLogManagerFactory(getConf(), getUri());
            try {
                return truncateStreams(factory);
            } finally {
                factory.close();
            }
        }

        private int truncateStreams(final DistributedLogManagerFactory factory) throws Exception {
            Collection<String> streamCollection = factory.enumerateAllLogsInNamespace();
            final List<String> streams = new ArrayList<String>();
            if (null != streamPrefix) {
                for (String s : streamCollection) {
                    if (s.startsWith(streamPrefix)) {
                        streams.add(s);
                    }
                }
            } else {
                streams.addAll(streamCollection);
            }
            if (0 == streams.size()) {
                return 0;
            }
            println("Streams : " + streams);
            if (!getForce() && !IOUtils.confirmPrompt("Are u sure to truncate " + streams.size() + " streams")) {
                return 0;
            }
            numThreads = Math.min(streams.size(), numThreads);
            final int numStreamsPerThreads = streams.size() / numThreads;
            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final int tid = i;
                threads[i] = new Thread("Truncate-" + i) {
                    @Override
                    public void run() {
                        try {
                            truncateStreams(factory, streams, tid, numStreamsPerThreads);
                            println("Thread " + tid + " finished.");
                        } catch (IOException e) {
                            println("Thread " + tid + " quits with exception : " + e.getMessage());
                        }
                    }
                };
                threads[i].start();
            }
            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }
            return 0;
        }

        private void truncateStreams(DistributedLogManagerFactory factory, List<String> streams,
                                     int tid, int numStreamsPerThreads) throws IOException {
            int startIdx = tid * numStreamsPerThreads;
            int endIdx = Math.min(streams.size(), (tid + 1) * numStreamsPerThreads);
            for (int i = startIdx; i < endIdx; i++) {
                String s = streams.get(i);
                DistributedLogManager dlm =
                        factory.createDistributedLogManagerWithSharedClients(s);
                try {
                    if (deleteStream) {
                        dlm.delete();
                    } else {
                        dlm.purgeLogsOlderThan(Long.MAX_VALUE);
                    }
                } finally {
                    dlm.close();
                }
            }
        }
    }

    protected static class ShowCommand extends PerStreamCommand {

        PartitionId partitionId = null;

        ShowCommand() {
            super("show", "show metadata of a given stream");
            options.addOption("p", "partition", true, "Partition of given stream");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("p")) {
                try {
                    partitionId = new PartitionId(Integer.parseInt(cmdline.getOptionValue("p")));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid partition " + cmdline.getOptionValue("p"));
                }
            }
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManager dlm = DistributedLogManagerFactory.createDistributedLogManager(
                    getStreamName(), getConf(), getUri());
            try {
                printMetadata(dlm);
            } finally {
                dlm.close();
            }
            return 0;
        }

        private void printMetadata(DistributedLogManager dlm) throws Exception {
            if (partitionId == null) {
                boolean hasStreams = false;
                // first tries to print metadata on unpartitioned streams
                try {
                    printMetadata(dlm, null);
                    hasStreams = true;
                } catch (LogNotFoundException lee) {
                    // nop
                }
                // tries all potential partitions
                int pid = 0;
                while (true) {
                    try {
                        printMetadata(dlm, new PartitionId(pid));
                        hasStreams = true;
                    } catch (LogNotFoundException lee) {
                        break;
                    }
                    ++pid;
                }
                if (!hasStreams) {
                    println("No streams found for " + getStreamName() + ".");
                }
                println("---------------------------------------------");
            } else {
                try {
                    printMetadata(dlm, partitionId);
                } catch (LogNotFoundException lee) {
                    println("No partition " + partitionId + " found.");
                }
            }
        }

        private void printMetadata(DistributedLogManager dlm, PartitionId pid) throws Exception {
            long firstTxnId = null == pid ? dlm.getFirstTxId() : dlm.getFirstTxId(pid);
            long lastTxnId = null == pid ? dlm.getLastTxId() : dlm.getLastTxId(pid);
            long recordCount = null == pid ? dlm.getLogRecordCount() : dlm.getLogRecordCount(pid);
            println("Stream " + (null == pid ? "<default>" : "partition " + pid)
                    + " : (first = " + firstTxnId + ", last = " + lastTxnId
                    + ", recordCount = " + recordCount + ")");
            if (null == pid) {
                List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
                for (LogSegmentLedgerMetadata segment : segments) {
                    println(segment.getLedgerSequenceNumber() + "\t: " + segment);
                }
            }
        }

        @Override
        protected String getUsage() {
            return "show [options]";
        }
    }

    static class CountCommand extends PerStreamCommand {

        DLSN startDLSN;
        DLSN endDLSN;

        protected CountCommand() {
            super("count", "count number records between dlsns");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            String[] args = cmdline.getArgs();
            if (args.length < 2) {
                throw new ParseException("Expected specifying start and end dlsns.");
            }
            try {
                startDLSN = parseDLSN(args[0]);
                endDLSN = parseDLSN(args[1]);
            } catch (IOException ioe) {
                throw new ParseException("Invalid dlsn found : " + ioe.getMessage());
            }
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManager dlm = DistributedLogManagerFactory.createDistributedLogManager(
                    getStreamName(), getConf(), getUri());
            int count = 0;
            try {
                LogReader reader = dlm.getInputStream(startDLSN);
                try {
                    LogRecordWithDLSN record = reader.readNext(false);
                    LogRecordWithDLSN preRecord = record;
                    println("first record : " + record);
                    while (null != record) {
                        if (record.getDlsn().compareTo(endDLSN) > 0) {
                            break;
                        }
                        ++count;
                        if (count % 1000 == 0) {
                            logger.info("read {} records from {}...", count, getStreamName());
                        }
                        preRecord = record;
                        record = reader.readNext(false);
                    }
                    println("last record : " + preRecord);
                    println("total is " + count + " records.");
                } finally {
                    reader.close();
                }
            } finally {
                dlm.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "count <start> <end>";
        }
    }

    public static class DeleteCommand extends PerStreamCommand {

        protected DeleteCommand() {
            super("delete", "delete a given stream");
        }

        @Override
        protected int runCmd() throws Exception {
            getConf().setZkAclId(getZkAclId());
            DistributedLogManager dlm = DistributedLogManagerFactory.createDistributedLogManager(
                    getStreamName(), getConf(), getUri());
            try {
                dlm.delete();
            } finally {
                dlm.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "delete";
        }
    }

    public static class CreateCommand extends PerDLCommand {

        final List<String> streams = new ArrayList<String>();

        String streamPrefix = null;
        String streamExpression = null;

        CreateCommand() {
            super("create", "create streams under a given namespace");
            options.addOption("r", "prefix", true, "Prefix of stream name. E.g. 'QuantumLeapTest-'.");
            options.addOption("e", "expression", true, "Expression to generate stream suffix. " +
                              "Currently we support range 'x-y', list 'x,y,z' and name 'xyz'");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("r")) {
                streamPrefix = cmdline.getOptionValue("r");
            }
            if (cmdline.hasOption("e")) {
                streamExpression = cmdline.getOptionValue("e");
            }
            if (null == streamPrefix || null == streamExpression) {
                throw new ParseException("Please specify stream prefix & expression.");
            }
        }

        protected void generateStreams(String streamPrefix, String streamExpression) throws ParseException {

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
                        int idx = Integer.parseInt(part);
                        streams.add(streamPrefix + idx);
                    }
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid stream suffix list : " + streamExpression);
                }
            } else {
                streams.add(streamPrefix + streamExpression);
            }
        }

        @Override
        protected int runCmd() throws Exception {
            generateStreams(streamPrefix, streamExpression);
            if (streams.isEmpty()) {
                println("Nothing to create.");
                return 0;
            }
            if (!getForce() && !IOUtils.confirmPrompt("Are u going to create streams : " + streams)) {
                return 0;
            }
            getConf().setZkAclId(getZkAclId());
            DistributedLogManagerFactory.createUnpartitionedStreams(getConf(), getUri(), streams);
            return 0;
        }

        @Override
        protected String getUsage() {
            return "create [options]";
        }

        protected void setPrefix(String prefix) {
            this.streamPrefix = prefix;
        }

        protected void setExpression(String expression) {
            this.streamExpression = expression;
        }
    }

    protected static class DumpCommand extends PerStreamCommand {

        PartitionId partitionId = null;
        boolean printHex = false;
        Long fromTxnId = null;
        DLSN fromDLSN = null;
        int count = 100;

        DumpCommand() {
            super("dump", "dump records of a given stream");
            options.addOption("p", "partition", true, "Partition of given stream");
            options.addOption("x", "hex", false, "Print record in hex format");
            options.addOption("o", "offset", true, "Txn ID to start dumping.");
            options.addOption("n", "seqno", true, "Sequence Number to start dumping");
            options.addOption("e", "eid", true, "Entry ID to start dumping");
            options.addOption("t", "slot", true, "Slot to start dumping");
            options.addOption("l", "limit", true, "Number of entries to dump. Default is 100.");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            printHex = cmdline.hasOption("x");
            if (cmdline.hasOption("p")) {
                try {
                    partitionId = new PartitionId(Integer.parseInt(cmdline.getOptionValue("p")));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid partition " + cmdline.getOptionValue("p"));
                }
            }
            if (cmdline.hasOption("o")) {
                try {
                    fromTxnId = Long.parseLong(cmdline.getOptionValue("o"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid txn id " + cmdline.getOptionValue("o"));
                }
            }
            if (cmdline.hasOption("l")) {
                try {
                    count = Integer.parseInt(cmdline.getOptionValue("l"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid count " + cmdline.getOptionValue("l"));
                }
                if (count <= 0) {
                    throw new ParseException("Negative count found : " + count);
                }
            }
            if (cmdline.hasOption("n")) {
                long seqno;
                try {
                    seqno = Long.parseLong(cmdline.getOptionValue("n"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid sequence number " + cmdline.getOptionValue("n"));
                }
                long eid;
                if (cmdline.hasOption("e")) {
                    eid = Long.parseLong(cmdline.getOptionValue("e"));
                } else {
                    eid = 0;
                }
                long slot;
                if (cmdline.hasOption("t")) {
                    slot = Long.parseLong(cmdline.getOptionValue("t"));
                } else {
                    slot = 0;
                }
                fromDLSN = new DLSN(seqno, eid, slot);
            }
            if (null == fromTxnId && null == fromDLSN) {
                throw new ParseException("No start Txn/DLSN is specified.");
            }
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManager dlm = DistributedLogManagerFactory.createDistributedLogManager(
                    getStreamName(), getConf(), getUri());
            try {
                LogReader reader;
                try {
                    if (null == partitionId) {
                        DLSN lastDLSN = dlm.getLastDLSNAsync().get();
                        println("Last DLSN : " + lastDLSN);
                        if (null == fromDLSN) {
                            reader = dlm.getInputStream(fromTxnId);
                        } else {
                            reader = dlm.getInputStream(fromDLSN);
                        }
                    } else {
                        DLSN lastDLSN = dlm.getLastDLSNAsync(partitionId).get();
                        println("Last DLSN : " + lastDLSN);
                        if (null == fromDLSN) {
                            reader = dlm.getInputStream(partitionId, fromTxnId);
                        } else {
                            reader = dlm.getInputStream(partitionId, fromDLSN);
                        }
                    }
                } catch (LogNotFoundException lee) {
                    println("No stream found to dump records.");
                    return 0;
                }
                try {
                    println("Dump records for " + getStreamName() + " (from = " + fromTxnId
                            + ", count = " + count + ", partition = " + partitionId + ")");
                    dumpRecords(reader);
                } finally {
                    reader.close();
                }
            } finally {
                dlm.close();
            }
            return 0;
        }

        private void dumpRecords(LogReader reader) throws Exception {
            int numRead = 0;
            LogRecord record = reader.readNext(false);
            while (record != null) {
                // dump the record
                dumpRecord(record);
                ++numRead;
                if (numRead >= count) {
                    break;
                }
                record = reader.readNext(false);
            }
            if (numRead == 0) {
                println("No records.");
            } else {
                println("------------------------------------------------");
            }
        }

        private void dumpRecord(LogRecord record) {
            println("------------------------------------------------");
            if (record instanceof LogRecordWithDLSN) {
                println("Record (txn = " + record.getTransactionId() + ", bytes = "
                        + record.getPayload().length + ", dlsn = "
                        + ((LogRecordWithDLSN) record).getDlsn() + ")");
            } else {
                println("Record (txn = " + record.getTransactionId() + ", bytes = "
                        + record.getPayload().length + ")");
            }
            println("");
            if (printHex) {
                println(Hex.encodeHexString(record.getPayload()));
            } else {
                println(new String(record.getPayload(), UTF_8));
            }
        }

        @Override
        protected String getUsage() {
            return "dump [options]";
        }

        protected void setFromTxnId(Long fromTxnId) {
            this.fromTxnId = fromTxnId;
        }
    }

    /**
     * TODO: refactor inspect & inspectstream
     * TODO: support force
     *
     * inspectstream -lac -gap (different options for different operations for a single stream)
     * inspect -lac -gap (inspect the namespace, which will use inspect stream)
     */
    static class InspectStreamCommand extends PerStreamCommand {

        InspectStreamCommand() {
            super("inspectstream", "Inspect a given stream to identify any metadata corruptions");
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManager dlm = DistributedLogManagerFactory.createDistributedLogManager(
                    getStreamName(), getConf(), getUri());
            try {
                return inspectAndRepair(dlm.getLogSegments());
            } finally {
                dlm.close();
            }
        }

        protected int inspectAndRepair(List<LogSegmentLedgerMetadata> segments) throws Exception {
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                    .sessionTimeoutMs(getConf().getZKSessionTimeoutMilliseconds())
                    .uri(getUri()).zkAclId(null).build();
            try {
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, getUri());
                BKDLConfig.propagateConfiguration(bkdlConfig, getConf());
                BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(getConf())
                        .zkServers(bkdlConfig.getBkZkServersForReader())
                        .ledgersPath(bkdlConfig.getBkLedgersPath())
                        .name("dlog")
                        .build();
                try {
                    List<LogSegmentLedgerMetadata> segmentsToRepair = inspectLogSegments(bkc, segments);
                    if (segmentsToRepair.isEmpty()) {
                        System.out.println("Stream is good. No log segments to repair.");
                        return 0;
                    }
                    System.out.println(segmentsToRepair.size() + " segments to repair : ");
                    System.out.println(segmentsToRepair);
                    System.out.println();
                    if (!IOUtils.confirmPrompt("Are u sure to repair them (Y/N): ")) {
                        System.out.println("Gave up! Bye!");
                        return -1;
                    }
                    repairLogSegments(zkc, bkc, segmentsToRepair);
                    return 0;
                } finally {
                    bkc.close();
                }
            } finally {
                zkc.close();
            }
        }

        protected List<LogSegmentLedgerMetadata> inspectLogSegments(
                BookKeeperClient bkc, List<LogSegmentLedgerMetadata> segments) throws Exception {
            List<LogSegmentLedgerMetadata> segmentsToRepair = new ArrayList<LogSegmentLedgerMetadata>();
            for (LogSegmentLedgerMetadata segment : segments) {
                if (!segment.isInProgress() && !inspectLogSegment(bkc, segment)) {
                    segmentsToRepair.add(segment);
                }
            }
            return segmentsToRepair;
        }

        /**
         * Inspect a given log segment.
         *
         * @param bkc
         *          bookkeeper client
         * @param metadata
         *          metadata of the log segment to
         * @return true if it is a good stream, false if the stream has inconsistent metadata.
         * @throws Exception
         */
        protected boolean inspectLogSegment(BookKeeperClient bkc,
                                            LogSegmentLedgerMetadata metadata) throws Exception {
            if (metadata.isInProgress()) {
                System.out.println("Skip inprogress log segment " + metadata);
                return true;
            }
            long ledgerId = metadata.getLedgerId();
            LedgerHandle lh = bkc.get().openLedger(ledgerId, BookKeeper.DigestType.CRC32,
                    getConf().getBKDigestPW().getBytes(UTF_8));
            LedgerHandle readLh = bkc.get().openLedger(ledgerId, BookKeeper.DigestType.CRC32,
                    getConf().getBKDigestPW().getBytes(UTF_8));
            LedgerReader lr = new LedgerReader(bkc.get());
            final AtomicReference<List<LedgerEntry>> entriesHolder = new AtomicReference<List<LedgerEntry>>(null);
            final AtomicInteger rcHolder = new AtomicInteger(-1234);
            final CountDownLatch doneLatch = new CountDownLatch(1);
            try {
                lr.forwardReadEntriesFromLastConfirmed(readLh, new BookkeeperInternalCallbacks.GenericCallback<List<LedgerEntry>>() {
                    @Override
                    public void operationComplete(int rc, List<LedgerEntry> entries) {
                        rcHolder.set(rc);
                        entriesHolder.set(entries);
                        doneLatch.countDown();
                    }
                });
                doneLatch.await();
                if (BKException.Code.OK != rcHolder.get()) {
                    throw BKException.create(rcHolder.get());
                }
                List<LedgerEntry> entries = entriesHolder.get();
                long lastEntryId;
                if (entries.isEmpty()) {
                    lastEntryId = LedgerHandle.INVALID_ENTRY_ID;
                } else {
                    LedgerEntry lastEntry = entries.get(entries.size() - 1);
                    lastEntryId = lastEntry.getEntryId();
                }
                if (lastEntryId != lh.getLastAddConfirmed()) {
                    System.out.println("Inconsistent Last Add Confirmed Found for LogSegment " + metadata.getLedgerSequenceNumber() + ": ");
                    System.out.println("\t metadata: " + metadata);
                    System.out.println("\t lac in ledger metadata is " + lh.getLastAddConfirmed() + ", but lac in bookies is " + lastEntryId);
                    return false;
                } else {
                    return true;
                }
            } finally {
                lh.close();
                readLh.close();
            }
        }

        protected void repairLogSegments(ZooKeeperClient zkc,
                                         BookKeeperClient bkc,
                                         List<LogSegmentLedgerMetadata> segments) throws Exception {
            BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bkc.get());
            try {
                MetadataUpdater metadataUpdater = ZkMetadataUpdater.createMetadataUpdater(zkc);
                for (LogSegmentLedgerMetadata segment : segments) {
                    repairLogSegment(bkAdmin, metadataUpdater, segment);
                }
            } finally {
                bkAdmin.close();
            }
        }

        protected void repairLogSegment(BookKeeperAdmin bkAdmin,
                                        MetadataUpdater metadataUpdater,
                                        LogSegmentLedgerMetadata segment) throws Exception {
            if (segment.isInProgress()) {
                System.out.println("Skip inprogress log segment " + segment);
                return;
            }
            LedgerHandle lh = bkAdmin.openLedger(segment.getLedgerId(), true);
            long lac = lh.getLastAddConfirmed();
            Enumeration<LedgerEntry> entries = lh.readEntries(lac, lac);
            if (!entries.hasMoreElements()) {
                throw new IOException("Entry " + lac + " isn't found for " + segment);
            }
            LedgerEntry lastEntry = entries.nextElement();
            LedgerEntryReader reader = new LedgerEntryReader("dlog", segment.getLedgerSequenceNumber(), lastEntry);
            LogRecordWithDLSN record = reader.readOp();
            LogRecordWithDLSN lastRecord = null;
            while (null != record) {
                lastRecord = record;
                record = reader.readOp();
            }
            if (null == lastRecord) {
                throw new IOException("No record found in entry " + lac + " for " + segment);
            }
            System.out.println("Updating last record for " + segment + " to " + lastRecord);
            if (!IOUtils.confirmPrompt("Are u sure to make the change (Y/N): ")) {
                System.out.println("Gave up on updating log segment " + segment);
                return;
            }
            metadataUpdater.updateLastRecord(segment, lastRecord);
        }

        @Override
        protected String getUsage() {
            return "inspectstream [options]";
        }
    }

    /**
     * Per Ledger Command, which parse common options for per ledger. e.g. ledger id.
     */
    abstract static class PerLedgerCommand extends PerDLCommand {

        protected long ledgerId;

        protected PerLedgerCommand(String name, String description) {
            super(name, description);
            options.addOption("l", "ledger", true, "Ledger ID");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (!cmdline.hasOption("l")) {
                throw new ParseException("No ledger provided.");
            }
            ledgerId = Long.parseLong(cmdline.getOptionValue("l"));
        }

        protected long getLedgerID() {
            return ledgerId;
        }

        protected void setLedgerId(long ledgerId) {
            this.ledgerId = ledgerId;
        }
    }

    protected static class ReadLastConfirmedCommand extends PerLedgerCommand {

        ReadLastConfirmedCommand() {
            super("readlac", "read last add confirmed for a given ledger");
        }

        @Override
        protected int runCmd() throws Exception {
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                    .sessionTimeoutMs(getConf().getZKSessionTimeoutMilliseconds())
                    .uri(getUri()).zkAclId(null).build();
            try {
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, getUri());
                BKDLConfig.propagateConfiguration(bkdlConfig, getConf());
                BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(getConf())
                        .zkServers(bkdlConfig.getBkZkServersForReader())
                        .ledgersPath(bkdlConfig.getBkLedgersPath())
                        .name("dlog")
                        .build();
                try {
                    LedgerHandle lh = bkc.get().openLedgerNoRecovery(getLedgerID(), BookKeeper.DigestType.CRC32,
                            dlConf.getBKDigestPW().getBytes(UTF_8));
                    try {
                        long lac = lh.readLastConfirmed();
                        println("LastAddConfirmed: " + lac);
                    } finally {
                        lh.close();
                    }
                } finally {
                    bkc.close();
                }
            } finally {
                zkc.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "readlac [options]";
        }
    }

    protected static class ReadEntriesCommand extends PerLedgerCommand {

        Long fromEntryId;
        Long untilEntryId;
        boolean readAllBookies = false;
        boolean readLac = false;

        ReadEntriesCommand() {
            super("readentries", "read entries for a given ledger");
            options.addOption("fid", "from", true, "Entry id to start reading");
            options.addOption("uid", "until", true, "Entry id to read until");
            options.addOption("bks", "all-bookies", false, "Read entry from all bookies");
            options.addOption("lac", "last-add-confirmed", false, "Return last add confirmed rather than entry payload");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("fid")) {
                fromEntryId = Long.parseLong(cmdline.getOptionValue("fid"));
            }
            if (cmdline.hasOption("uid")) {
                untilEntryId = Long.parseLong(cmdline.getOptionValue("uid"));
            }
            readAllBookies = cmdline.hasOption("bks");
            readLac = cmdline.hasOption("lac");
        }

        @Override
        protected int runCmd() throws Exception {
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                    .sessionTimeoutMs(getConf().getZKSessionTimeoutMilliseconds())
                    .uri(getUri()).zkAclId(null).build();
            try {
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, getUri());
                BKDLConfig.propagateConfiguration(bkdlConfig, getConf());
                BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                        .dlConfig(getConf())
                        .zkServers(bkdlConfig.getBkZkServersForReader())
                        .ledgersPath(bkdlConfig.getBkLedgersPath())
                        .name("dlog")
                        .build();
                try {
                    LedgerHandle lh = bkc.get().openLedgerNoRecovery(getLedgerID(), BookKeeper.DigestType.CRC32,
                            dlConf.getBKDigestPW().getBytes(UTF_8));
                    try {
                        if (null == fromEntryId) {
                            fromEntryId = 0L;
                        }
                        if (null == untilEntryId) {
                            untilEntryId = lh.readLastConfirmed();
                        }
                        if (untilEntryId >= fromEntryId) {
                            if (readAllBookies) {
                                LedgerReader lr = new LedgerReader(bkc.get());
                                if (readLac) {
                                    readLacsFromAllBookies(lr, lh, fromEntryId, untilEntryId);
                                } else {
                                    readEntriesFromAllBookies(lr, lh, fromEntryId, untilEntryId);
                                }
                            } else {
                                simpleReadEntries(lh, fromEntryId, untilEntryId);
                            }
                        } else {
                            System.out.println("No entries.");
                        }
                    } finally {
                        lh.close();
                    }
                } finally {
                    bkc.close();
                }
            } finally {
                zkc.close();
            }
            return 0;
        }

        private void readEntriesFromAllBookies(LedgerReader ledgerReader, LedgerHandle lh, long fromEntryId, long untilEntryId)
                throws Exception {
            for (long eid = fromEntryId; eid <= untilEntryId; ++eid) {
                final CountDownLatch doneLatch = new CountDownLatch(1);
                final AtomicReference<Set<LedgerReader.ReadResult<InputStream>>> resultHolder =
                        new AtomicReference<Set<LedgerReader.ReadResult<InputStream>>>();
                ledgerReader.readEntriesFromAllBookies(lh, eid, new BookkeeperInternalCallbacks.GenericCallback<Set<LedgerReader.ReadResult<InputStream>>>() {
                    @Override
                    public void operationComplete(int rc, Set<LedgerReader.ReadResult<InputStream>> readResults) {
                        if (BKException.Code.OK == rc) {
                            resultHolder.set(readResults);
                        } else {
                            resultHolder.set(null);
                        }
                        doneLatch.countDown();
                    }
                });
                doneLatch.await();
                Set<LedgerReader.ReadResult<InputStream>> readResults = resultHolder.get();
                if (null == readResults) {
                    throw new IOException("Failed to read entry " + eid);
                }
                System.out.println("\t" + eid + "\t:");
                for (LedgerReader.ReadResult<InputStream> rr : readResults) {
                    System.out.println("\tbookie=" + rr.getBookieAddress());
                    System.out.println("\t-------------------------------");
                    if (BKException.Code.OK == rr.getResultCode()) {
                        LedgerEntryReader reader = new LedgerEntryReader("dlog", lh.getId(), eid, rr.getValue());
                        printEntry(reader);
                    } else {
                        System.out.println("status = " + BKException.getMessage(rr.getResultCode()));
                    }
                    System.out.println("\t-------------------------------");
                }
            }
        }

        private void readLacsFromAllBookies(LedgerReader ledgerReader, LedgerHandle lh, long fromEntryId, long untilEntryId)
                throws Exception {
            for (long eid = fromEntryId; eid <= untilEntryId; ++eid) {
                final CountDownLatch doneLatch = new CountDownLatch(1);
                final AtomicReference<Set<LedgerReader.ReadResult<Long>>> resultHolder =
                        new AtomicReference<Set<LedgerReader.ReadResult<Long>>>();
                ledgerReader.readLacs(lh, eid, new BookkeeperInternalCallbacks.GenericCallback<Set<LedgerReader.ReadResult<Long>>>() {
                    @Override
                    public void operationComplete(int rc, Set<LedgerReader.ReadResult<Long>> readResults) {
                        if (BKException.Code.OK == rc) {
                            resultHolder.set(readResults);
                        } else {
                            resultHolder.set(null);
                        }
                        doneLatch.countDown();
                    }
                });
                doneLatch.await();
                Set<LedgerReader.ReadResult<Long>> readResults = resultHolder.get();
                if (null == readResults) {
                    throw new IOException("Failed to read entry " + eid);
                }
                System.out.println("\t" + eid + "\t:");
                for (LedgerReader.ReadResult<Long> rr : readResults) {
                    System.out.println("\tbookie=" + rr.getBookieAddress());
                    System.out.println("\t-------------------------------");
                    if (BKException.Code.OK == rr.getResultCode()) {
                        System.out.println("Eid = " + rr.getEntryId() + ", Lac = " + rr.getValue());
                    } else {
                        System.out.println("status = " + BKException.getMessage(rr.getResultCode()));
                    }
                    System.out.println("\t-------------------------------");
                }
            }
        }

        private void simpleReadEntries(LedgerHandle lh, long fromEntryId, long untilEntryId) throws Exception {
            Enumeration<LedgerEntry> entries = lh.readEntries(fromEntryId, untilEntryId);
            long i = fromEntryId;
            System.out.println("Entries:");
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                System.out.println("\t" + i  + "(eid=" + entry.getEntryId() + ")\t: ");
                LedgerEntryReader reader = new LedgerEntryReader("dlog", 0L, entry);
                printEntry(reader);
                ++i;
            }
        }

        private void printEntry(LedgerEntryReader reader) throws Exception {
            LogRecordWithDLSN record = reader.readOp();
            while (null != record) {
                System.out.println("\t" + record);
                System.out.println(new String(record.getPayload(), UTF_8));
                System.out.println();
                record = reader.readOp();
            }
        }

        @Override
        protected String getUsage() {
            return "readentries [options]";
        }
    }

    public DistributedLogTool() {
        super();
        addCommand(new CreateCommand());
        addCommand(new ListCommand());
        addCommand(new DumpCommand());
        addCommand(new ShowCommand());
        addCommand(new DeleteCommand());
        addCommand(new DeleteAllocatorPoolCommand());
        addCommand(new TruncateCommand());
        addCommand(new InspectCommand());
        addCommand(new InspectStreamCommand());
        addCommand(new ReadLastConfirmedCommand());
        addCommand(new ReadEntriesCommand());
        addCommand(new CountCommand());
    }

    @Override
    protected String getName() {
        return "dlog_tool";
    }

}
