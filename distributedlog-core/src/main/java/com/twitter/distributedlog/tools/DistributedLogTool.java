package com.twitter.distributedlog.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.twitter.distributedlog.BKDistributedLogNamespace;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAccessor;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.LedgerReader;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LedgerEntryReader;
import com.twitter.distributedlog.LogNotFoundException;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.auditor.DLAuditor;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.LedgerAllocatorUtils;
import com.twitter.distributedlog.metadata.BKDLConfig;
import com.twitter.distributedlog.metadata.MetadataUpdater;
import com.twitter.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import com.twitter.distributedlog.util.SchedulerUtils;
import com.twitter.util.Await;

import static com.google.common.base.Charsets.UTF_8;

@SuppressWarnings("deprecation")
public class DistributedLogTool extends Tool {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogTool.class);

    static final List<String> EMPTY_LIST = Lists.newArrayList();

    static int compareByCompletionTime(long time1, long time2) {
        return time1 > time2 ? 1 : (time1 < time2 ? -1 : 0);
    }

    static final Comparator<LogSegmentMetadata> LOGSEGMENT_COMPARATOR_BY_TIME = new Comparator<LogSegmentMetadata>() {
        @Override
        public int compare(LogSegmentMetadata o1, LogSegmentMetadata o2) {
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

    static DLSN parseDLSN(String dlsnStr) throws ParseException {
        if (dlsnStr.equals("InitialDLSN")) {
            return DLSN.InitialDLSN;
        }
        String[] parts = dlsnStr.split(",");
        if (parts.length != 3) {
            throw new ParseException("Invalid dlsn : " + dlsnStr);
        }
        try {
            return new DLSN(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]));
        } catch (Exception nfe) {
            throw new ParseException("Invalid dlsn : " + dlsnStr);
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
        protected com.twitter.distributedlog.DistributedLogManagerFactory factory = null;

        protected PerDLCommand(String name, String description) {
            super(name, description);
            dlConf = new DistributedLogConfiguration();
            // Tools are allowed to read old metadata as long as they can interpret it
            dlConf.setDLLedgerMetadataSkipMinVersionCheck(true);
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
            try {
                return runCmd();
            } finally {
                synchronized (this) {
                    if (null != factory) {
                        factory.close();
                    }
                }
            }
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

        protected synchronized com.twitter.distributedlog.DistributedLogManagerFactory getFactory() throws IOException {
            if (null == this.factory) {
                this.factory = new com.twitter.distributedlog.DistributedLogManagerFactory(getConf(), getUri());
                logger.info("Construct DLM : uri = {}", getUri());
            }
            return this.factory;
        }

        protected LogSegmentMetadataStore getLogSegmentMetadataStore() throws IOException {
            DistributedLogNamespace namespace = getFactory().getNamespace();
            assert(namespace instanceof BKDistributedLogNamespace);
            return ((BKDistributedLogNamespace) namespace).getWriterSegmentMetadataStore();
        }

        protected ZooKeeperClient getZooKeeperClient() throws IOException {
            DistributedLogNamespace namespace = getFactory().getNamespace();
            assert(namespace instanceof BKDistributedLogNamespace);
            return ((BKDistributedLogNamespace) namespace).getSharedWriterZKCForDL();
        }

        protected BookKeeperClient getBookKeeperClient() throws IOException {
            DistributedLogNamespace namespace = getFactory().getNamespace();
            assert(namespace instanceof BKDistributedLogNamespace);
            return ((BKDistributedLogNamespace) namespace).getReaderBKC();
        }
    }

    /**
     * Base class for simple command with no resource setup requirements.
     */
    public abstract static class SimpleCommand extends OptsCommand {

        protected final Options options = new Options();

        SimpleCommand(String name, String description) {
            super(name, description);
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
            return runSimpleCmd();
        }

        abstract protected int runSimpleCmd() throws Exception;

        abstract protected void parseCommandLine(CommandLine cmdline) throws ParseException;

        @Override
        protected Options getOptions() {
            return options;
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

        int concurrency = 1;
        String allocationPoolPath = DistributedLogConstants.ALLOCATION_POOL_NODE;

        DeleteAllocatorPoolCommand() {
            super("delete_allocator_pool", "Delete allocator pool for a given distributedlog instance");
            options.addOption("t", "concurrency", true, "Concurrency on deleting allocator pool.");
            options.addOption("ap", "allocation-pool-path", true, "Ledger Allocation Pool Path");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("t")) {
                concurrency = Integer.parseInt(cmdline.getOptionValue("t"));
                if (concurrency <= 0) {
                    throw new ParseException("Invalid concurrency value : " + concurrency);
                }
            }
            if (cmdline.hasOption("ap")) {
                allocationPoolPath = cmdline.getOptionValue("ap");
                if (!allocationPoolPath.startsWith(".") || !allocationPoolPath.contains("allocation")) {
                    throw new ParseException("Invalid allocation pool path : " + allocationPoolPath);
                }
            }
        }

        @Override
        protected int runCmd() throws Exception {
            String rootPath = getUri().getPath() + "/" + allocationPoolPath;
            final ScheduledExecutorService allocationExecutor = Executors.newSingleThreadScheduledExecutor();
            ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
            try {
                List<String> pools = getZooKeeperClient().get().getChildren(rootPath, false);
                final LinkedBlockingQueue<String> poolsToDelete = new LinkedBlockingQueue<String>();
                if (getForce() || IOUtils.confirmPrompt("Are you sure you want to delete allocator pools : " + pools)) {
                    for (String pool : pools) {
                        poolsToDelete.add(rootPath + "/" + pool);
                    }
                    final CountDownLatch doneLatch = new CountDownLatch(concurrency);
                    for (int i = 0; i < concurrency; i++) {
                        final int tid = i;
                        executorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                while (!poolsToDelete.isEmpty()) {
                                    String poolPath = poolsToDelete.poll();
                                    if (null == poolPath) {
                                        break;
                                    }
                                    try {
                                        LedgerAllocator allocator =
                                                LedgerAllocatorUtils.createLedgerAllocatorPool(poolPath, 0, getConf(),
                                                        getZooKeeperClient(), getBookKeeperClient(),
                                                        allocationExecutor);
                                        if (null == allocator) {
                                            println("ERROR: use zk34 version to delete allocator pool : " + poolPath + " .");
                                        } else {
                                            allocator.delete();
                                            println("Deleted allocator pool : " + poolPath + " .");
                                        }
                                    } catch (IOException ioe) {
                                        println("Failed to delete allocator pool " + poolPath + " : " + ioe.getMessage());
                                    }
                                }
                                doneLatch.countDown();
                                println("Thread " + tid + " is done.");
                            }
                        });
                    }
                    doneLatch.await();
                }
            } finally {
                executorService.shutdown();
                allocationExecutor.shutdown();
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
            if (printMetadata) {
                printStreamsWithMetadata(getFactory());
            } else {
                printStreams(getFactory());
            }
            return 0;
        }

        protected void printStreamsWithMetadata(com.twitter.distributedlog.DistributedLogManagerFactory factory)
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

        protected void printStreams(com.twitter.distributedlog.DistributedLogManagerFactory factory) throws Exception {
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
        boolean checkInprogressOnly = false;

        InspectCommand() {
            super("inspect", "Inspect streams under a given dl uri to find any potential corruptions");
            options.addOption("t", "threads", true, "Number threads to do inspection.");
            options.addOption("ft", "filter", true, "Stream filter by prefix");
            options.addOption("i", "inprogress", false, "Print inprogress log segments only");
            options.addOption("d", "dump", false, "Dump entries of inprogress log segments");
            options.addOption("ot", "orderbytime", false, "Order the log segments by completion time");
            options.addOption("pso", "print-stream-only", false, "Print streams only");
            options.addOption("cio", "check-inprogress-only", false, "Check duplicated inprogress only");
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
            checkInprogressOnly = cmdline.hasOption("cio");
        }

        @Override
        protected int runCmd() throws Exception {
            SortedMap<String, List<Pair<LogSegmentMetadata, List<String>>>> corruptedCandidates =
                    new TreeMap<String, List<Pair<LogSegmentMetadata, List<String>>>>();
            inspectStreams(corruptedCandidates);
            System.out.println("Corrupted Candidates : ");
            if (printStreamsOnly) {
                System.out.println(corruptedCandidates.keySet());
                return 0;
            }
            for (Map.Entry<String, List<Pair<LogSegmentMetadata, List<String>>>> entry : corruptedCandidates.entrySet()) {
                System.out.println(entry.getKey() + " : \n");
                List<LogSegmentMetadata> segments = new ArrayList<LogSegmentMetadata>(entry.getValue().size());
                for (Pair<LogSegmentMetadata, List<String>> pair : entry.getValue()) {
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
        }

        private void inspectStreams(final SortedMap<String, List<Pair<LogSegmentMetadata, List<String>>>> corruptedCandidates)
                throws Exception {
            Collection<String> streamCollection = getFactory().enumerateAllLogsInNamespace();
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
                            inspectStreams(streams, tid, numStreamsPerThreads, corruptedCandidates);
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

        private void inspectStreams(List<String> streams,
                                    int tid,
                                    int numStreamsPerThreads,
                                    SortedMap<String, List<Pair<LogSegmentMetadata, List<String>>>> corruptedCandidates)
                throws Exception {
            int startIdx = tid * numStreamsPerThreads;
            int endIdx = Math.min(streams.size(), (tid + 1) * numStreamsPerThreads);
            for (int i = startIdx; i < endIdx; i++) {
                String s = streams.get(i);
                BookKeeperClient bkc = getBookKeeperClient();
                DistributedLogManager dlm =
                        getFactory().createDistributedLogManagerWithSharedClients(s);
                try {
                    List<LogSegmentMetadata> segments = dlm.getLogSegments();
                    if (segments.size() <= 1) {
                        continue;
                    }
                    boolean isCandidate = false;
                    if (checkInprogressOnly) {
                        Set<Long> inprogressSeqNos = new HashSet<Long>();
                        for (LogSegmentMetadata segment : segments) {
                            if (segment.isInProgress()) {
                                inprogressSeqNos.add(segment.getLogSegmentSequenceNumber());
                            }
                        }
                        for (LogSegmentMetadata segment : segments) {
                            if (!segment.isInProgress() && inprogressSeqNos.contains(segment.getLogSegmentSequenceNumber())) {
                                isCandidate = true;
                            }
                        }
                    } else {
                        LogSegmentMetadata firstSegment = segments.get(0);
                        long lastSeqNo = firstSegment.getLogSegmentSequenceNumber();

                        for (int j = 1; j < segments.size(); j++) {
                            LogSegmentMetadata nextSegment = segments.get(j);
                            if (lastSeqNo + 1 != nextSegment.getLogSegmentSequenceNumber()) {
                                isCandidate = true;
                                break;
                            }
                            ++lastSeqNo;
                        }
                    }
                    if (isCandidate) {
                        if (orderByTime) {
                            Collections.sort(segments, LOGSEGMENT_COMPARATOR_BY_TIME);
                        }
                        List<Pair<LogSegmentMetadata, List<String>>> ledgers =
                                new ArrayList<Pair<LogSegmentMetadata, List<String>>>();
                        for (LogSegmentMetadata seg : segments) {
                            LogSegmentMetadata segment = seg;
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
            return truncateStreams(getFactory());
        }

        private int truncateStreams(final com.twitter.distributedlog.DistributedLogManagerFactory factory) throws Exception {
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

        private void truncateStreams(com.twitter.distributedlog.DistributedLogManagerFactory factory, List<String> streams,
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

    public static class SimpleBookKeeperClient {
        BookKeeperClient bkc;
        ZooKeeperClient zkc;

        public SimpleBookKeeperClient(DistributedLogConfiguration conf, URI uri) {
            try {
                zkc = ZooKeeperClientBuilder.newBuilder()
                    .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                    .zkAclId(conf.getZkAclId())
                    .uri(uri)
                    .build();
                BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, uri);
                BKDLConfig.propagateConfiguration(bkdlConfig, conf);
                bkc = BookKeeperClientBuilder.newBuilder()
                        .zkc(zkc)
                        .dlConfig(conf)
                        .ledgersPath(bkdlConfig.getBkLedgersPath())
                        .name("dlog")
                        .build();
            } catch (Exception e) {
                close();
            }
        }
        public BookKeeperClient client() {
            return bkc;
        }
        public void close() {
            if (null != bkc) {
                bkc.close();
            }
            if (null != zkc) {
                zkc.close();
            }
        }
    }

    protected static class ShowCommand extends PerStreamCommand {

        SimpleBookKeeperClient bkc = null;
        boolean listSegments = true;
        boolean listEppStats = false;
        long firstLid = 0;
        long lastLid = -1;

        ShowCommand() {
            super("show", "show metadata of a given stream and list segments");
            options.addOption("ns", "no-log-segments", false, "Do not list log segment metadata");
            options.addOption("lp", "placement-stats", false, "Show ensemble placement stats");
            options.addOption("fl", "first-ledger", true, "First log sement no");
            options.addOption("ll", "last-ledger", true, "Last log sement no");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("fl")) {
                try {
                    firstLid = Long.parseLong(cmdline.getOptionValue("fl"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid ledger id " + cmdline.getOptionValue("fl"));
                }
            }
            if (firstLid < 0) {
                throw new IllegalArgumentException("Invalid ledger id " + firstLid);
            }
            if (cmdline.hasOption("ll")) {
                try {
                    lastLid = Long.parseLong(cmdline.getOptionValue("ll"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid ledger id " + cmdline.getOptionValue("ll"));
                }
            }
            if (lastLid != -1 && firstLid > lastLid) {
                throw new IllegalArgumentException("Invalid ledger ids " + firstLid + " " + lastLid);
            }
            listSegments = !cmdline.hasOption("ns");
            listEppStats = cmdline.hasOption("lp");
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManager dlm = getFactory().createDistributedLogManagerWithSharedClients(getStreamName());
            try {
                if (listEppStats) {
                    bkc = new SimpleBookKeeperClient(getConf(), getUri());
                }
                printMetadata(dlm);
            } finally {
                dlm.close();
                if (null != bkc) {
                    bkc.close();
                }
            }
            return 0;
        }

        private void printMetadata(DistributedLogManager dlm) throws Exception {
            printHeader(dlm);
            if (listSegments) {
                println("Ledgers : ");
                List<LogSegmentMetadata> segments = dlm.getLogSegments();
                for (LogSegmentMetadata segment : segments) {
                    if (include(segment)) {
                        printLedgerRow(segment);
                    }
                }
            }
        }

        private void printHeader(DistributedLogManager dlm) throws Exception {
            DLSN firstDlsn = Await.result(dlm.getFirstDLSNAsync());
            boolean endOfStreamMarked = dlm.isEndOfStreamMarked();
            DLSN lastDlsn = dlm.getLastDLSN();
            long firstTxnId = dlm.getFirstTxId();
            long lastTxnId = dlm.getLastTxId();
            long recordCount = dlm.getLogRecordCount();
            String result = String.format("Stream : (firstTxId=%d, lastTxid=%d, firstDlsn=%s, lastDlsn=%s, endOfStreamMarked=%b, recordCount=%d)",
                firstTxnId, lastTxnId, getDlsnName(firstDlsn), getDlsnName(lastDlsn), endOfStreamMarked, recordCount);
            println(result);
            if (listEppStats) {
                printEppStatsHeader(dlm);
            }
        }

        boolean include(LogSegmentMetadata segment) {
            return (firstLid <= segment.getLogSegmentSequenceNumber() && (lastLid == -1 || lastLid >= segment.getLogSegmentSequenceNumber()));
        }

        private void printEppStatsHeader(DistributedLogManager dlm) throws Exception {
            String label = "Ledger Placement :";
            println(label);
            Map<BookieSocketAddress, Integer> totals = new HashMap<BookieSocketAddress, Integer>();
            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            for (LogSegmentMetadata segment : segments) {
                if (include(segment)) {
                    merge(totals, getBookieStats(segment));
                }
            }
            List<Map.Entry<BookieSocketAddress, Integer>> entries = new ArrayList<Map.Entry<BookieSocketAddress, Integer>>(totals.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<BookieSocketAddress, Integer>>() {
                @Override
                public int compare(Map.Entry<BookieSocketAddress, Integer> o1, Map.Entry<BookieSocketAddress, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            int width = 0;
            int totalEntries = 0;
            for (Map.Entry<BookieSocketAddress, Integer> entry : entries) {
                width = Math.max(width, label.length() + 1 + entry.getKey().toString().length());
                totalEntries += entry.getValue();
            }
            for (Map.Entry<BookieSocketAddress, Integer> entry : entries) {
                println(String.format("%"+width+"s\t%6.2f%%\t\t%d", entry.getKey(), entry.getValue()*1.0/totalEntries, entry.getValue()));
            }
        }

        private void printLedgerRow(LogSegmentMetadata segment) throws Exception {
            println(segment.getLogSegmentSequenceNumber() + "\t: " + segment);
        }

        private Map<BookieSocketAddress, Integer> getBookieStats(LogSegmentMetadata segment) throws Exception {
            Map<BookieSocketAddress, Integer> stats = new HashMap<BookieSocketAddress, Integer>();
            LedgerHandle lh = bkc.client().get().openLedgerNoRecovery(segment.getLedgerId(), BookKeeper.DigestType.CRC32,
                    getConf().getBKDigestPW().getBytes(UTF_8));
            long eidFirst = 0;
            for (SortedMap.Entry<Long, ArrayList<BookieSocketAddress>> entry : LedgerReader.bookiesForLedger(lh).entrySet()) {
                long eidLast = entry.getKey().longValue();
                long count = eidLast - eidFirst + 1;
                for (BookieSocketAddress bookie : entry.getValue()) {
                    merge(stats, bookie, (int) count);
                }
                eidFirst = eidLast;
            }
            return stats;
        }

        void merge(Map<BookieSocketAddress, Integer> m, BookieSocketAddress bookie, Integer count) {
            if (m.containsKey(bookie)) {
                m.put(bookie, count + m.get(bookie).intValue());
            } else {
                m.put(bookie, count);
            }
        }

        void merge(Map<BookieSocketAddress, Integer> m1, Map<BookieSocketAddress, Integer> m2) {
            for (Map.Entry<BookieSocketAddress, Integer> entry : m2.entrySet()) {
                merge(m1, entry.getKey(), entry.getValue());
            }
        }

        String getDlsnName(DLSN dlsn) {
            if (dlsn.equals(DLSN.InvalidDLSN)) {
                return "InvalidDLSN";
            }
            return dlsn.toString();
        }

        @Override
        protected String getUsage() {
            return "show [options]";
        }
    }

    static class CountCommand extends PerStreamCommand {

        DLSN startDLSN = null;
        DLSN endDLSN = null;

        protected CountCommand() {
            super("count", "count number records between dlsns");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            String[] args = cmdline.getArgs();
            if (args.length < 1) {
                throw new ParseException("Must specify at least start dlsn.");
            }
            if (args.length >= 1) {
                startDLSN = parseDLSN(args[0]);
            }
            if (args.length >= 2) {
                endDLSN = parseDLSN(args[1]);
            }
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManager dlm = getFactory().createDistributedLogManagerWithSharedClients(getStreamName());
            try {
                long count = 0;
                if (null == endDLSN) {
                    count = countToLastRecord(dlm);
                } else {
                    count = countFromStartToEnd(dlm);
                }
                println("total is " + count + " records.");
                return 0;
            } finally {
                dlm.close();
            }
        }

        int countFromStartToEnd(DistributedLogManager dlm) throws Exception {
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
                } finally {
                    reader.close();
                }
            } finally {
                dlm.close();
            }
            return count;
        }

        long countToLastRecord(DistributedLogManager dlm) throws Exception {
            return Await.result(dlm.getLogRecordCountAsync(startDLSN)).longValue();
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
            DistributedLogManager dlm = getFactory().createDistributedLogManagerWithSharedClients(getStreamName());
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

    public static class DeleteLedgersCommand extends PerDLCommand {

        private final List<Long> ledgers = new ArrayList<Long>();

        int numThreads = 1;

        protected DeleteLedgersCommand() {
            super("delete_ledgers", "delete given ledgers");
            options.addOption("l", "ledgers", true, "List of ledgers, separated by comma");
            options.addOption("lf", "ledgers-file", true, "File of list of ledgers, each line has a ledger id");
            options.addOption("t", "concurrency", true, "Number of threads to run deletions");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("l") && cmdline.hasOption("lf")) {
                throw new ParseException("Please specify ledgers either use list or use file only.");
            }
            if (!cmdline.hasOption("l") && !cmdline.hasOption("lf")) {
                throw new ParseException("No ledgers specified. Please specify ledgers either use list or use file only.");
            }
            if (cmdline.hasOption("l")) {
                String ledgersStr = cmdline.getOptionValue("l");
                String[] ledgerStrs = ledgersStr.split(",");
                for (String ledgerStr : ledgerStrs) {
                    ledgers.add(Long.parseLong(ledgerStr));
                }
            }
            if (cmdline.hasOption("lf")) {
                BufferedReader br = null;
                try {

                    br = new BufferedReader(new InputStreamReader(
                            new FileInputStream(new File(cmdline.getOptionValue("lf"))), UTF_8.name()));
                    String line;
                    while ((line = br.readLine()) != null) {
                        ledgers.add(Long.parseLong(line));
                    }
                } catch (FileNotFoundException e) {
                    throw new ParseException("No ledgers file " + cmdline.getOptionValue("lf") + " found.");
                } catch (IOException e) {
                    throw new ParseException("Invalid ledgers file " + cmdline.getOptionValue("lf") + " found.");
                } finally {
                    if (null != br) {
                        try {
                            br.close();
                        } catch (IOException e) {
                            // no-op
                        }
                    }
                }
            }
            if (cmdline.hasOption("t")) {
                numThreads = Integer.parseInt(cmdline.getOptionValue("t"));
            }
        }

        @Override
        protected String getUsage() {
            return "delete_ledgers [options]";
        }

        @Override
        protected int runCmd() throws Exception {
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            try {
                final AtomicInteger numLedgers = new AtomicInteger(0);
                final CountDownLatch doneLatch = new CountDownLatch(numThreads);
                final AtomicInteger numFailures = new AtomicInteger(0);
                final LinkedBlockingQueue<Long> ledgerQueue =
                        new LinkedBlockingQueue<Long>();
                ledgerQueue.addAll(ledgers);
                for (int i = 0; i < numThreads; i++) {
                    final int tid = i;
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            while (true) {
                                Long ledger = ledgerQueue.poll();
                                if (null == ledger) {
                                    break;
                                }
                                try {
                                    getBookKeeperClient().get().deleteLedger(ledger);
                                    int numLedgersDeleted = numLedgers.incrementAndGet();
                                    if (numLedgersDeleted % 1000 == 0) {
                                        println("Deleted " + numLedgersDeleted + " ledgers.");
                                    }
                                } catch (BKException.BKNoSuchLedgerExistsException e) {
                                    int numLedgersDeleted = numLedgers.incrementAndGet();
                                    if (numLedgersDeleted % 1000 == 0) {
                                        println("Deleted " + numLedgersDeleted + " ledgers.");
                                    }
                                } catch (Exception e) {
                                    numFailures.incrementAndGet();
                                    break;
                                }
                            }
                            doneLatch.countDown();
                            println("Thread " + tid + " quits");
                        }
                    });
                }
                doneLatch.await();
                if (numFailures.get() > 0) {
                    throw new IOException("Encounter " + numFailures.get() + " failures during deleting ledgers");
                }
            } finally {
                executorService.shutdown();
            }
            return 0;
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
            for (String stream : streams) {
                getFactory().getNamespace().createLog(stream);
            }
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

        boolean printHex = false;
        boolean skipPayload = false;
        Long fromTxnId = null;
        DLSN fromDLSN = null;
        int count = 100;

        DumpCommand() {
            super("dump", "dump records of a given stream");
            options.addOption("x", "hex", false, "Print record in hex format");
            options.addOption("sp", "skip-payload", false, "Skip printing the payload of the record");
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
            skipPayload = cmdline.hasOption("sp");
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
            DistributedLogManager dlm = getFactory().createDistributedLogManagerWithSharedClients(getStreamName());
            long totalCount = dlm.getLogRecordCount();
            try {
                AsyncLogReader reader;
                Object startOffset;
                try {
                    DLSN lastDLSN = Await.result(dlm.getLastDLSNAsync());
                    println("Last DLSN : " + lastDLSN);
                    if (null == fromDLSN) {
                        reader = dlm.getAsyncLogReader(fromTxnId);
                        startOffset = fromTxnId;
                    } else {
                        reader = dlm.getAsyncLogReader(fromDLSN);
                        startOffset = fromDLSN;
                    }
                } catch (LogNotFoundException lee) {
                    println("No stream found to dump records.");
                    return 0;
                }
                try {
                    println(String.format("Dump records for %s (from = %s, dump count = %d, total records = %d)",
                            getStreamName(), startOffset, count, totalCount));

                    dumpRecords(reader);
                } finally {
                    reader.close();
                }
            } finally {
                dlm.close();
            }
            return 0;
        }

        private void dumpRecords(AsyncLogReader reader) throws Exception {
            int numRead = 0;
            LogRecord record = Await.result(reader.readNext());
            while (record != null) {
                // dump the record
                dumpRecord(record);
                ++numRead;
                if (numRead >= count) {
                    break;
                }
                record = Await.result(reader.readNext());
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

            if (skipPayload) {
                return;
            }

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
            DistributedLogManager dlm = getFactory().createDistributedLogManagerWithSharedClients(getStreamName());
            try {
                return inspectAndRepair(dlm.getLogSegments());
            } finally {
                dlm.close();
            }
        }

        protected int inspectAndRepair(List<LogSegmentMetadata> segments) throws Exception {
            LogSegmentMetadataStore metadataStore = getLogSegmentMetadataStore();
            ZooKeeperClient zkc = getZooKeeperClient();
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, getUri());
            BKDLConfig.propagateConfiguration(bkdlConfig, getConf());
            BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                    .dlConfig(getConf())
                    .zkServers(bkdlConfig.getBkZkServersForReader())
                    .ledgersPath(bkdlConfig.getBkLedgersPath())
                    .name("dlog")
                    .build();
            try {
                List<LogSegmentMetadata> segmentsToRepair = inspectLogSegments(bkc, segments);
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
                repairLogSegments(metadataStore, bkc, segmentsToRepair);
                return 0;
            } finally {
                bkc.close();
            }
        }

        protected List<LogSegmentMetadata> inspectLogSegments(
                BookKeeperClient bkc, List<LogSegmentMetadata> segments) throws Exception {
            List<LogSegmentMetadata> segmentsToRepair = new ArrayList<LogSegmentMetadata>();
            for (LogSegmentMetadata segment : segments) {
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
                                            LogSegmentMetadata metadata) throws Exception {
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
                    System.out.println("Inconsistent Last Add Confirmed Found for LogSegment " + metadata.getLogSegmentSequenceNumber() + ": ");
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

        protected void repairLogSegments(LogSegmentMetadataStore metadataStore,
                                         BookKeeperClient bkc,
                                         List<LogSegmentMetadata> segments) throws Exception {
            BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bkc.get());
            try {
                MetadataUpdater metadataUpdater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(
                        getConf(), metadataStore);
                for (LogSegmentMetadata segment : segments) {
                    repairLogSegment(bkAdmin, metadataUpdater, segment);
                }
            } finally {
                bkAdmin.close();
            }
        }

        protected void repairLogSegment(BookKeeperAdmin bkAdmin,
                                        MetadataUpdater metadataUpdater,
                                        LogSegmentMetadata segment) throws Exception {
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
            LedgerEntryReader reader = new LedgerEntryReader("dlog",
                                                             segment.getLogSegmentSequenceNumber(),
                                                             lastEntry,
                                                             LogSegmentMetadata.supportsEnvelopedEntries(segment.getVersion()),
                                                             segment.getStartSequenceId(),
                                                             NullStatsLogger.INSTANCE);
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

    static interface BKCommandRunner {
        int run(ZooKeeperClient zkc, BookKeeperClient bkc) throws Exception;
    }

    abstract static class PerBKCommand extends PerDLCommand {

        protected PerBKCommand(String name, String description) {
            super(name, description);
        }

        @Override
        protected int runCmd() throws Exception {
            return runBKCommand(new BKCommandRunner() {
                @Override
                public int run(ZooKeeperClient zkc, BookKeeperClient bkc) throws Exception {
                    return runBKCmd(zkc, bkc);
                }
            });
        }

        protected int runBKCommand(BKCommandRunner runner) throws Exception {
            return runner.run(getZooKeeperClient(), getBookKeeperClient());
        }

        abstract protected int runBKCmd(ZooKeeperClient zkc, BookKeeperClient bkc) throws Exception;
    }

    static class RecoverCommand extends PerBKCommand {

        final List<Long> ledgers = new ArrayList<Long>();
        boolean query = false;
        boolean dryrun = false;
        boolean skipOpenLedgers = false;
        boolean fenceOnly = false;
        int fenceRate = 1;
        int concurrency = 1;
        final Set<BookieSocketAddress> bookiesSrc = new HashSet<BookieSocketAddress>();
        int partition = 0;
        int numPartitions = 0;

        RecoverCommand() {
            super("recover", "Recover the ledger data that stored on failed bookies");
            options.addOption("l", "ledger", true, "Specific ledger to recover");
            options.addOption("lf", "ledgerfile", true, "File contains ledgers list");
            options.addOption("q", "query", false, "Query the ledgers that contain given bookies");
            options.addOption("d", "dryrun", false, "Print the recovery plan w/o actually recovering");
            options.addOption("cy", "concurrency", true, "Number of ledgers could be recovered in parallel");
            options.addOption("sk", "skipOpenLedgers", false, "Skip recovering open ledgers");
            options.addOption("p", "partition", true, "partition");
            options.addOption("n", "num-partitions", true, "num partitions");
            options.addOption("fo", "fence-only", true, "fence the ledgers only w/o re-replicating entries");
            options.addOption("fr", "fence-rate", true, "rate on fencing ledgers");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            query = cmdline.hasOption("q");
            force = cmdline.hasOption("f");
            dryrun = cmdline.hasOption("d");
            skipOpenLedgers = cmdline.hasOption("sk");
            fenceOnly = cmdline.hasOption("fo");
            if (cmdline.hasOption("l")) {
                String[] lidStrs = cmdline.getOptionValue("l").split(",");
                try {
                    for (String lidStr : lidStrs) {
                        ledgers.add(Long.parseLong(lidStr));
                    }
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid ledger id provided : " + cmdline.getOptionValue("l"));
                }
            }
            if (cmdline.hasOption("lf")) {
                String file = cmdline.getOptionValue("lf");
                try {
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(file), UTF_8.name()));
                    try {
                        String line = br.readLine();

                        while (line != null) {
                            ledgers.add(Long.parseLong(line));
                            line = br.readLine();
                        }
                    } finally {
                        br.close();
                    }
                } catch (IOException e) {
                    throw new ParseException("Invalid ledgers file provided : " + file);
                }
            }
            if (cmdline.hasOption("cy")) {
                try {
                    concurrency = Integer.parseInt(cmdline.getOptionValue("cy"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid concurrency provided : " + cmdline.getOptionValue("cy"));
                }
            }
            if (cmdline.hasOption("p")) {
                partition = Integer.parseInt(cmdline.getOptionValue("p"));
            }
            if (cmdline.hasOption("n")) {
                numPartitions = Integer.parseInt(cmdline.getOptionValue("n"));
            }
            if (cmdline.hasOption("fr")) {
                fenceRate = Integer.parseInt(cmdline.getOptionValue("fr"));
            }
            // Get bookies list to recover
            String[] args = cmdline.getArgs();
            final String[] bookieStrs = args[0].split(",");
            for (String bookieStr : bookieStrs) {
                final String bookieStrParts[] = bookieStr.split(":");
                if (bookieStrParts.length != 2) {
                    throw new ParseException("BookieSrcs has invalid bookie address format (host:port expected) : "
                            + bookieStr);
                }
                try {
                    bookiesSrc.add(new BookieSocketAddress(bookieStrParts[0],
                            Integer.parseInt(bookieStrParts[1])));
                } catch (NumberFormatException nfe) {
                    throw new ParseException("Invalid ledger id provided : " + cmdline.getOptionValue("l"));
                }
            }
        }

        @Override
        protected int runBKCmd(ZooKeeperClient zkc, BookKeeperClient bkc) throws Exception {
            BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bkc.get());
            try {
                if (query) {
                    return bkQuery(bkAdmin, bookiesSrc);
                }
                if (fenceOnly) {
                    return bkFence(bkc, ledgers, fenceRate);
                }
                if (!force) {
                    System.err.println("Bookies : " + bookiesSrc);
                    if (!IOUtils.confirmPrompt("Are you sure to recover them : (Y/N)")) {
                        System.err.println("Give up!");
                        return -1;
                    }
                }
                if (!ledgers.isEmpty()) {
                    System.err.println("Ledgers : " + ledgers);
                    long numProcessed = 0;
                    Iterator<Long> ledgersIter = ledgers.iterator();
                    LinkedBlockingQueue<Long> ledgersToProcess = new LinkedBlockingQueue<Long>();
                    while (ledgersIter.hasNext()) {
                        long lid = ledgersIter.next();
                        if (numPartitions <=0 || (numPartitions > 0 && lid % numPartitions == partition)) {
                            ledgersToProcess.add(lid);
                            ++numProcessed;
                        }
                        if (ledgersToProcess.size() == 10000) {
                            System.out.println("Processing " + numProcessed + " ledgers");
                            bkRecovery(ledgersToProcess, bookiesSrc, dryrun, skipOpenLedgers);
                            ledgersToProcess.clear();
                            System.out.println("Processed " + numProcessed + " ledgers");
                        }
                    }
                    if (!ledgersToProcess.isEmpty()) {
                        System.out.println("Processing " + numProcessed + " ledgers");
                        bkRecovery(ledgersToProcess, bookiesSrc, dryrun, skipOpenLedgers);
                        System.out.println("Processed " + numProcessed + " ledgers");
                    }
                    System.out.println("Done.");
                    CountDownLatch latch = new CountDownLatch(1);
                    latch.await();
                    return 0;
                }
                return bkRecovery(bkAdmin, bookiesSrc, dryrun, skipOpenLedgers);
            } finally {
                bkAdmin.close();
            }
        }

        private int bkFence(final BookKeeperClient bkc, List<Long> ledgers, int fenceRate) throws Exception {
            if (ledgers.isEmpty()) {
                System.out.println("Nothing to fence. Done.");
                return 0;
            }
            ExecutorService executorService = Executors.newCachedThreadPool();
            final RateLimiter rateLimiter = RateLimiter.create(fenceRate);
            final byte[] passwd = getConf().getBKDigestPW().getBytes(UTF_8);
            final CountDownLatch latch = new CountDownLatch(ledgers.size());
            final AtomicInteger numPendings = new AtomicInteger(ledgers.size());
            final LinkedBlockingQueue<Long> ledgersQueue = new LinkedBlockingQueue<Long>();
            ledgersQueue.addAll(ledgers);

            for (int i = 0; i < concurrency; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (!ledgersQueue.isEmpty()) {
                            rateLimiter.acquire();
                            Long lid = ledgersQueue.poll();
                            if (null == lid) {
                                break;
                            }
                            System.out.println("Fencing ledger " + lid);
                            int numRetries = 3;
                            while (numRetries > 0) {
                                try {
                                    LedgerHandle lh = bkc.get().openLedger(lid, BookKeeper.DigestType.CRC32, passwd);
                                    lh.close();
                                    System.out.println("Fenced ledger " + lid + ", " + numPendings.decrementAndGet() + " left.");
                                    latch.countDown();
                                } catch (BKException.BKNoSuchLedgerExistsException bke) {
                                    System.out.println("Skipped fence non-exist ledger " + lid + ", " + numPendings.decrementAndGet() + " left.");
                                    latch.countDown();
                                } catch (BKException.BKLedgerRecoveryException lre) {
                                    --numRetries;
                                    continue;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    break;
                                }
                                numRetries = 0;
                            }
                        }
                        System.err.println("Thread exits");
                    }
                });
            }
            latch.await();
            SchedulerUtils.shutdownScheduler(executorService, 2, TimeUnit.MINUTES);
            return 0;
        }

        private int bkQuery(BookKeeperAdmin bkAdmin, Set<BookieSocketAddress> bookieAddrs)
                throws InterruptedException, BKException {
            SortedMap<Long, LedgerMetadata> ledgersContainBookies =
                    bkAdmin.getLedgersContainBookies(bookieAddrs);
            System.err.println("NOTE: Bookies in inspection list are marked with '*'.");
            for (Map.Entry<Long, LedgerMetadata> ledger : ledgersContainBookies.entrySet()) {
                System.out.println("ledger " + ledger.getKey() + " : " + ledger.getValue().getState());
                Map<Long, Integer> numBookiesToReplacePerEnsemble =
                        inspectLedger(ledger.getValue(), bookieAddrs);
                System.out.print("summary: [");
                for (Map.Entry<Long, Integer> entry : numBookiesToReplacePerEnsemble.entrySet()) {
                    System.out.print(entry.getKey() + "=" + entry.getValue() + ", ");
                }
                System.out.println("]");
                System.out.println();
            }
            System.err.println("Done");
            return 0;
        }

        private Map<Long, Integer> inspectLedger(LedgerMetadata metadata, Set<BookieSocketAddress> bookiesToInspect) {
            Map<Long, Integer> numBookiesToReplacePerEnsemble = new TreeMap<Long, Integer>();
            for (Map.Entry<Long, ArrayList<BookieSocketAddress>> ensemble : metadata.getEnsembles().entrySet()) {
                ArrayList<BookieSocketAddress> bookieList = ensemble.getValue();
                System.out.print(ensemble.getKey() + ":\t");
                int numBookiesToReplace = 0;
                for (BookieSocketAddress bookie: bookieList) {
                    System.out.print(bookie.toString());
                    if (bookiesToInspect.contains(bookie)) {
                        System.out.print("*");
                        ++numBookiesToReplace;
                    } else {
                        System.out.print(" ");
                    }
                    System.out.print(" ");
                }
                System.out.println();
                numBookiesToReplacePerEnsemble.put(ensemble.getKey(), numBookiesToReplace);
            }
            return numBookiesToReplacePerEnsemble;
        }

        private int bkRecovery(final LinkedBlockingQueue<Long> ledgers, final Set<BookieSocketAddress> bookieAddrs,
                               final boolean dryrun, final boolean skipOpenLedgers)
                throws Exception {
            return runBKCommand(new BKCommandRunner() {
                @Override
                public int run(ZooKeeperClient zkc, BookKeeperClient bkc) throws Exception {
                    BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bkc.get());
                    try {
                        bkRecovery(bkAdmin, ledgers, bookieAddrs, dryrun, skipOpenLedgers);
                        return 0;
                    } finally {
                        bkAdmin.close();
                    }
                }
            });
        }

        private int bkRecovery(final BookKeeperAdmin bkAdmin, final LinkedBlockingQueue<Long> ledgers,
                               final Set<BookieSocketAddress> bookieAddrs,
                               final boolean dryrun, final boolean skipOpenLedgers)
                throws InterruptedException, BKException {
            final AtomicInteger numPendings = new AtomicInteger(ledgers.size());
            final ExecutorService executorService = Executors.newCachedThreadPool();
            final CountDownLatch doneLatch = new CountDownLatch(concurrency);
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    while (!ledgers.isEmpty()) {
                        long lid = -1L;
                        try {
                            lid = ledgers.take();
                            System.out.println("Recovering ledger " + lid);
                            bkAdmin.recoverBookieData(lid, bookieAddrs, dryrun, skipOpenLedgers);
                            System.out.println("Recovered ledger completed : " + lid + ", " + numPendings.decrementAndGet() + " left");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            doneLatch.countDown();
                            break;
                        } catch (BKException ke) {
                            System.out.println("Recovered ledger failed : " + lid + ", rc = " + BKException.getMessage(ke.getCode()));
                        }
                    }
                    doneLatch.countDown();
                }
            };
            for (int i = 0; i < concurrency; i++) {
                executorService.submit(r);
            }
            doneLatch.await();
            SchedulerUtils.shutdownScheduler(executorService, 2, TimeUnit.MINUTES);
            return 0;
        }

        private int bkRecovery(BookKeeperAdmin bkAdmin, Set<BookieSocketAddress> bookieAddrs,
                               boolean dryrun, boolean skipOpenLedgers)
                throws InterruptedException, BKException {
            bkAdmin.recoverBookieData(bookieAddrs, dryrun, skipOpenLedgers);
            return 0;
        }

        @Override
        protected String getUsage() {
            return "recover [options] <bookiesSrc>";
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

    protected static class RecoverLedgerCommand extends PerLedgerCommand {

        RecoverLedgerCommand() {
            super("recoverledger", "force recover ledger");
        }

        @Override
        protected int runCmd() throws Exception {
            LedgerHandle lh = getBookKeeperClient().get().openLedgerNoRecovery(
                    getLedgerID(), BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes(UTF_8));
            final CountDownLatch doneLatch = new CountDownLatch(1);
            final AtomicInteger resultHolder = new AtomicInteger(-1234);
            BookkeeperInternalCallbacks.GenericCallback<Void> recoverCb =
                    new BookkeeperInternalCallbacks.GenericCallback<Void>() {
                @Override
                public void operationComplete(int rc, Void result) {
                    resultHolder.set(rc);
                    doneLatch.countDown();
                }
            };
            try {
                BookKeeperAccessor.forceRecoverLedger(lh, recoverCb);
                doneLatch.await();
                if (BKException.Code.OK != resultHolder.get()) {
                    throw BKException.create(resultHolder.get());
                }
            } finally {
                lh.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "recoverledger [options]";
        }
    }

    protected static class ReadLastConfirmedCommand extends PerLedgerCommand {

        ReadLastConfirmedCommand() {
            super("readlac", "read last add confirmed for a given ledger");
        }

        @Override
        protected int runCmd() throws Exception {
            LedgerHandle lh = getBookKeeperClient().get().openLedgerNoRecovery(
                    getLedgerID(), BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes(UTF_8));
            try {
                long lac = lh.readLastConfirmed();
                println("LastAddConfirmed: " + lac);
            } finally {
                lh.close();
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
        boolean printHex = false;
        boolean skipPayload = false;
        boolean readAllBookies = false;
        boolean readLac = false;
        boolean corruptOnly = false;

        int metadataVersion = LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION;

        ReadEntriesCommand() {
            super("readentries", "read entries for a given ledger");
            options.addOption("x", "hex", false, "Print record in hex format");
            options.addOption("sp", "skip-payload", false, "Skip printing the payload of the record");
            options.addOption("fid", "from", true, "Entry id to start reading");
            options.addOption("uid", "until", true, "Entry id to read until");
            options.addOption("bks", "all-bookies", false, "Read entry from all bookies");
            options.addOption("lac", "last-add-confirmed", false, "Return last add confirmed rather than entry payload");
            options.addOption("ver", "metadata-version", true, "The log segment metadata version to use");
            options.addOption("bad", "corrupt-only", false, "Display info for corrupt entries only");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            printHex = cmdline.hasOption("x");
            skipPayload = cmdline.hasOption("sp");
            if (cmdline.hasOption("fid")) {
                fromEntryId = Long.parseLong(cmdline.getOptionValue("fid"));
            }
            if (cmdline.hasOption("uid")) {
                untilEntryId = Long.parseLong(cmdline.getOptionValue("uid"));
            }
            if (cmdline.hasOption("ver")) {
                metadataVersion = Integer.parseInt(cmdline.getOptionValue("ver"));
            }
            corruptOnly = cmdline.hasOption("bad");
            readAllBookies = cmdline.hasOption("bks");
            readLac = cmdline.hasOption("lac");
        }

        @Override
        protected int runCmd() throws Exception {
            LedgerHandle lh = getBookKeeperClient().get().openLedgerNoRecovery(getLedgerID(), BookKeeper.DigestType.CRC32,
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
                        LedgerReader lr = new LedgerReader(getBookKeeperClient().get());
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
                boolean printHeader = true;
                for (LedgerReader.ReadResult<InputStream> rr : readResults) {
                    if (corruptOnly) {
                        if (BKException.Code.DigestMatchException == rr.getResultCode()) {
                            if (printHeader) {
                                System.out.println("\t" + eid + "\t:");
                                printHeader = false;
                            }
                            System.out.println("\tbookie=" + rr.getBookieAddress());
                            System.out.println("\t-------------------------------");
                            System.out.println("status = " + BKException.getMessage(rr.getResultCode()));
                            System.out.println("\t-------------------------------");
                        }
                    } else {
                        if (printHeader) {
                            System.out.println("\t" + eid + "\t:");
                            printHeader = false;
                        }
                        System.out.println("\tbookie=" + rr.getBookieAddress());
                        System.out.println("\t-------------------------------");
                        if (BKException.Code.OK == rr.getResultCode()) {
                            LedgerEntryReader reader = new LedgerEntryReader("dlog", lh.getId(), eid, rr.getValue(),
                                                                             LogSegmentMetadata.supportsEnvelopedEntries(metadataVersion),
                                                                             0L, NullStatsLogger.INSTANCE);
                            printEntry(reader);
                        } else {
                            System.out.println("status = " + BKException.getMessage(rr.getResultCode()));
                        }
                        System.out.println("\t-------------------------------");
                    }
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
                LedgerEntryReader reader = new LedgerEntryReader("dlog", 0L, entry,
                                                                 LogSegmentMetadata.supportsEnvelopedEntries(metadataVersion),
                                                                 0L, NullStatsLogger.INSTANCE);
                printEntry(reader);
                ++i;
            }
        }

        private void printEntry(LedgerEntryReader reader) throws Exception {
            LogRecordWithDLSN record = reader.readOp();
            while (null != record) {
                System.out.println("\t" + record);
                if (!skipPayload) {
                    if (printHex) {
                        println(Hex.encodeHexString(record.getPayload()));
                    } else {
                        println(new String(record.getPayload(), UTF_8));
                    }
                }
                println("");
                record = reader.readOp();
            }
        }

        @Override
        protected String getUsage() {
            return "readentries [options]";
        }
    }

    protected static abstract class AuditCommand extends OptsCommand {

        protected final Options options = new Options();
        protected final DistributedLogConfiguration dlConf;
        protected final List<URI> uris = new ArrayList<URI>();
        protected String zkAclId = null;
        protected boolean force = false;

        protected AuditCommand(String name, String description) {
            super(name, description);
            dlConf = new DistributedLogConfiguration();
            options.addOption("u", "uris", true, "List of distributedlog uris, separated by comma");
            options.addOption("c", "conf", true, "DistributedLog Configuration File");
            options.addOption("a", "zk-acl-id", true, "ZooKeeper ACL ID");
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
            String urisStr = cmdline.getOptionValue("u");
            for (String uriStr : urisStr.split(",")) {
                uris.add(URI.create(uriStr));
            }
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

        protected List<URI> getUris() {
            return uris;
        }

        protected String getZkAclId() {
            return zkAclId;
        }

        protected boolean getForce() {
            return force;
        }

    }

    static class AuditLedgersCommand extends AuditCommand {

        String ledgersFilePrefix;
        final List<List<String>> allocationPaths =
                new ArrayList<List<String>>();

        AuditLedgersCommand() {
            super("audit_ledgers", "Audit ledgers between bookkeeper and DL uris");
            options.addOption("lf", "ledgers-file", true, "Prefix of filename to store ledgers");
            options.addOption("ap", "allocation-paths", true, "Allocation paths per uri. E.g ap10;ap11,ap20");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("lf")) {
                ledgersFilePrefix = cmdline.getOptionValue("lf");
            } else {
                throw new ParseException("No file specified to store leak ledgers");
            }
            if (cmdline.hasOption("ap")) {
                String[] aps = cmdline.getOptionValue("ap").split(",");
                for(String ap : aps) {
                    List<String> list = new ArrayList<String>();
                    String[] array = ap.split(";");
                    Collections.addAll(list, array);
                    allocationPaths.add(list);
                }
            } else {
                throw new ParseException("No allocation paths provided.");
            }
        }

        void dumpLedgers(Set<Long> ledgers, File targetFile) throws Exception {
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(targetFile), UTF_8.name()));
            try {
                for (Long ledger : ledgers) {
                    pw.println(ledger);
                }
            } finally {
                pw.close();
            }
            println("Dump " + ledgers.size() + " ledgers to file : " + targetFile);
        }

        @Override
        protected int runCmd() throws Exception {
            if (!getForce() && !IOUtils.confirmPrompt("Are u sure to audit uris : "
                    + getUris() + ", allocation paths = " + allocationPaths)) {
                println("ByeBye");
                return 0;
            }

            DLAuditor dlAuditor = new DLAuditor(getConf());
            try {
                Pair<Set<Long>, Set<Long>> bkdlLedgers = dlAuditor.collectLedgers(getUris(), allocationPaths);
                dumpLedgers(bkdlLedgers.getLeft(), new File(ledgersFilePrefix + "-bkledgers.txt"));
                dumpLedgers(bkdlLedgers.getRight(), new File(ledgersFilePrefix + "-dlledgers.txt"));
                dumpLedgers(Sets.difference(bkdlLedgers.getLeft(), bkdlLedgers.getRight()),
                            new File(ledgersFilePrefix + "-leakledgers.txt"));
            } finally {
                dlAuditor.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "audit_ledgers [options]";
        }
    }

    public static class AuditDLSpaceCommand extends PerDLCommand {

        private String regex = null;

        AuditDLSpaceCommand() {
            super("audit_dl_space", "Audit stream space usage for a given dl uri");
            options.addOption("groupByRegex", true, "Group by the result of applying the regex to stream name");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("groupByRegex")) {
                regex = cmdline.getOptionValue("groupByRegex");
            }
        }

        @Override
        protected int runCmd() throws Exception {
            DLAuditor dlAuditor = new DLAuditor(getConf());
            try {
                Map<String, Long> streamSpaceMap = dlAuditor.calculateStreamSpaceUsage(getUri());
                if (null != regex) {
                    printGroupByRegexSpaceUsage(streamSpaceMap, regex);
                } else {
                    printSpaceUsage(streamSpaceMap);
                }
            } finally {
                dlAuditor.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "audit_dl_space [options]";
        }

        private void printSpaceUsage(Map<String, Long> spaceMap) throws Exception {
            for (Map.Entry<String, Long> entry : spaceMap.entrySet()) {
                System.out.println(entry.getKey() + "\t" + entry.getValue());
            }
        }

        private void printGroupByRegexSpaceUsage(Map<String, Long> streamSpaceMap, String regex) throws Exception {
            Pattern pattern = Pattern.compile(regex);
            Map<String, Long> groupedUsageMap = new HashMap<String, Long>();
            for (Map.Entry<String, Long> entry : streamSpaceMap.entrySet()) {
                Matcher matcher = pattern.matcher(entry.getKey());
                String key = entry.getKey();
                boolean matches = matcher.matches();
                if (matches) {
                    key = matcher.group(1);
                }
                Long value = entry.getValue();
                if (groupedUsageMap.containsKey(key)) {
                    value += groupedUsageMap.get(key);
                }
                groupedUsageMap.put(key, value);
            }
            printSpaceUsage(groupedUsageMap);
        }
    }

    public static class AuditBKSpaceCommand extends PerDLCommand {

        AuditBKSpaceCommand() {
            super("audit_bk_space", "Audit bk space usage for a given dl uri");
        }

        @Override
        protected int runCmd() throws Exception {
            DLAuditor dlAuditor = new DLAuditor(getConf());
            try {
                long spaceUsage = dlAuditor.calculateLedgerSpaceUsage(uri);
                System.out.println("bookkeeper ledgers space usage \t " + spaceUsage);
            } finally {
                dlAuditor.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "audit_bk_space [options]";
        }
    }

    protected static class TruncateStreamCommand extends PerStreamCommand {

        DLSN dlsn = DLSN.InvalidDLSN;

        TruncateStreamCommand() {
            super("truncate_stream", "truncate a stream at a specific position");
            options.addOption("dlsn", true, "Truncate all records older than this dlsn");
        }

        public void setDlsn(DLSN dlsn) {
            this.dlsn = dlsn;
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("dlsn")) {
                dlsn = parseDLSN(cmdline.getOptionValue("dlsn"));
            }
        }

        @Override
        protected int runCmd() throws Exception {
            getConf().setZkAclId(getZkAclId());
            return truncateStream(getFactory(), getStreamName(), dlsn);
        }

        private int truncateStream(final com.twitter.distributedlog.DistributedLogManagerFactory factory, String streamName, DLSN dlsn) throws Exception {
            DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
            try {
                long totalRecords = dlm.getLogRecordCount();
                long recordsAfterTruncate = Await.result(dlm.getLogRecordCountAsync(dlsn));
                long recordsToTruncate = totalRecords - recordsAfterTruncate;
                if (!getForce() && !IOUtils.confirmPrompt("Are you sure you want to truncate " + streamName + " at dlsn " + dlsn + " (" + recordsToTruncate + " records)?")) {
                    return 0;
                } else {
                    AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
                    try {
                        if (!Await.result(writer.truncate(dlsn))) {
                            println("Failed to truncate.");
                        }
                        return 0;
                    } finally {
                        writer.close();
                    }
                }
            } catch (Exception ex) {
                println("Failed to truncate " + ex);
                return 1;
            } finally {
                dlm.close();
            }
        }
    }

    public static class DeserializeDLSNCommand extends SimpleCommand {

        String base64Dlsn = "";

        DeserializeDLSNCommand() {
            super("deserialize_dlsn", "Deserialize DLSN");
            options.addOption("b64", "base64", true, "Base64 encoded dlsn");
        }

        public void setBase64DLSN(String base64Dlsn) {
            base64Dlsn = base64Dlsn;
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            if (cmdline.hasOption("b64")) {
                base64Dlsn = cmdline.getOptionValue("b64");
            } else {
                throw new IllegalArgumentException("Argument b64 is required");
            }
        }

        @Override
        protected int runSimpleCmd() throws Exception {
            println(DLSN.deserialize(base64Dlsn).toString());
            return 0;
        }
    }

    public static class SerializeDLSNCommand extends SimpleCommand {

        private DLSN dlsn = DLSN.InitialDLSN;
        private boolean hex = false;

        SerializeDLSNCommand() {
            super("serialize_dlsn", "Serialize DLSN. Default format is base64 string.");
            options.addOption("dlsn", true, "DLSN in comma separated format to serialize");
            options.addOption("x", "hex", false, "Emit hex-encoded string DLSN instead of base 64");
        }

        public void setDLSN(DLSN dlsn) {
            dlsn = dlsn;
        }

        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            if (cmdline.hasOption("dlsn")) {
                dlsn = parseDLSN(cmdline.getOptionValue("dlsn"));
            }
            hex = cmdline.hasOption("x");
        }

        @Override
        protected int runSimpleCmd() throws Exception {
            if (hex) {
                byte[] bytes = dlsn.serializeBytes();
                String hexString = Hex.encodeHexString(bytes);
                println(hexString);
            } else {
                println(dlsn.serialize());
            }
            return 0;
        }
    }

    public DistributedLogTool() {
        super();
        addCommand(new AuditBKSpaceCommand());
        addCommand(new AuditLedgersCommand());
        addCommand(new AuditDLSpaceCommand());
        addCommand(new CreateCommand());
        addCommand(new CountCommand());
        addCommand(new DeleteCommand());
        addCommand(new DeleteAllocatorPoolCommand());
        addCommand(new DeleteLedgersCommand());
        addCommand(new DumpCommand());
        addCommand(new InspectCommand());
        addCommand(new InspectStreamCommand());
        addCommand(new ListCommand());
        addCommand(new ReadLastConfirmedCommand());
        addCommand(new ReadEntriesCommand());
        addCommand(new RecoverCommand());
        addCommand(new RecoverLedgerCommand());
        addCommand(new ShowCommand());
        addCommand(new TruncateCommand());
        addCommand(new TruncateStreamCommand());
        addCommand(new DeserializeDLSNCommand());
        addCommand(new SerializeDLSNCommand());
    }

    @Override
    protected String getName() {
        return "dlog_tool";
    }

}
