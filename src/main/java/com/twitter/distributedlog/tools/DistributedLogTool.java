package com.twitter.distributedlog.tools;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogEmptyException;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.PartitionId;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.bk.LedgerAllocator;
import com.twitter.distributedlog.bk.LedgerAllocatorUtils;
import com.twitter.distributedlog.metadata.BKDLConfig;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;

public class DistributedLogTool extends Tool {

    /**
     * Per DL Command, which parses basic options. e.g. uri.
     */
    abstract class PerDLCommand extends OptsCommand {

        protected Options options = new Options();
        protected final DistributedLogConfiguration dlConf;
        protected URI uri;

        protected PerDLCommand(String name, String description) {
            super(name, description);
            dlConf = new DistributedLogConfiguration();
            options.addOption("u", "uri", true, "DistributedLog URI");
            options.addOption("c", "conf", true, "DistributedLog Configuration File");
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
        }

        protected DistributedLogConfiguration getConf() {
            return dlConf;
        }

        protected URI getUri() {
            return uri;
        }
    }

    /**
     * Per Stream Command, which parse common options for per stream. e.g. stream name.
     */
    abstract class PerStreamCommand extends PerDLCommand {

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
    }

    class DeleteAllocatorPoolCommand extends PerDLCommand {

        DeleteAllocatorPoolCommand() {
            super("delete_allocator_pool", "Delete allocator pool for a given distributedlog instance");
        }

        @Override
        protected int runCmd() throws Exception {
            String rootPath = getUri().getPath() + "/" + DistributedLogConstants.ALLOCATION_POOL_NODE;
            ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                    .sessionTimeoutMs(dlConf.getZKSessionTimeoutMilliseconds())
                    .uri(getUri()).buildNew();
            BKDLConfig bkdlConfig = BKDLConfig.resolveDLConfig(zkc, getUri());
            BookKeeperClient bkc = BookKeeperClientBuilder.newBuilder()
                    .dlConfig(getConf()).bkdlConfig(bkdlConfig).name("dlog_tool").build();
            try {
                List<String> pools = zkc.get().getChildren(rootPath, false);
                if (IOUtils.confirmPrompt("Are u sure to delete allocator pools : " + pools)) {
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
                bkc.release();
                zkc.close();
            }
            return 0;
        }

        @Override
        protected String getUsage() {
            return "delete_allocator_pool";
        }
    }

    class ListCommand extends PerDLCommand {

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
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
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

    class TruncateCommand extends PerDLCommand {

        int numThreads = 1;
        String streamPrefix = null;
        boolean deleteStream = false;
        boolean force = false;

        TruncateCommand() {
            super("truncate", "truncate streams under a given dl uri");
            options.addOption("t", "threads", true, "Number threads to do truncation");
            options.addOption("f", "filter", true, "Stream filter by prefix");
            options.addOption("d", "delete", false, "Delete Stream");
            options.addOption("x", "force", false, "Force execute");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            if (cmdline.hasOption("t")) {
                numThreads = Integer.parseInt(cmdline.getOptionValue("t"));
            }
            if (cmdline.hasOption("f")) {
                streamPrefix = cmdline.getOptionValue("f");
            }
            if (cmdline.hasOption("d")) {
                deleteStream = true;
            }
            force = cmdline.hasOption("x");
        }

        @Override
        protected String getUsage() {
            return "truncate [options]";
        }

        @Override
        protected int runCmd() throws Exception {
            final DistributedLogManagerFactory factory =
                    new DistributedLogManagerFactory(getConf(), getUri());
            int rc = truncateStreams(factory);
            System.exit(rc);
            return rc;
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
            if (!force && !IOUtils.confirmPrompt("Are u sure to truncate " + streams.size() + " streams")) {
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

    class ShowCommand extends PerStreamCommand {

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
                } catch (LogEmptyException lee) {
                    // nop
                }
                // tries all potential partitions
                int pid = 0;
                while (true) {
                    try {
                        printMetadata(dlm, new PartitionId(pid));
                        hasStreams = true;
                    } catch (LogEmptyException lee) {
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
                } catch (LogEmptyException lee) {
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
        }

        @Override
        protected String getUsage() {
            return "show [options]";
        }
    }

    class DeleteCommand extends PerStreamCommand {

        protected DeleteCommand() {
            super("delete", "delete a given stream");
        }

        @Override
        protected int runCmd() throws Exception {
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

    class CreateCommand extends PerDLCommand {

        final List<String> streams = new ArrayList<String>();

        CreateCommand() {
            super("create", "create streams under a given namespace");
            options.addOption("r", "prefix", true, "Prefix of stream name. E.g. 'QuantumLeapTest-'.");
            options.addOption("e", "expression", true, "Expression to generate stream suffix. " +
                              "Currently we support range 'x-y', list 'x,y,z' and name 'xyz'");
        }

        @Override
        protected void parseCommandLine(CommandLine cmdline) throws ParseException {
            super.parseCommandLine(cmdline);
            String streamPrefix = null;
            String streamExpression = null;
            if (cmdline.hasOption("r")) {
                streamPrefix = cmdline.getOptionValue("r");
            }
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
            if (streams.isEmpty()) {
                println("Nothing to create.");
                return 0;
            }
            if (!IOUtils.confirmPrompt("Are u going to create streams : " + streams)) {
                return 0;
            }
            DistributedLogManagerFactory.createUnpartitionedStreams(getConf(), getUri(), streams);
            return 0;
        }

        @Override
        protected String getUsage() {
            return "create [options]";
        }
    }

    class DumpCommand extends PerStreamCommand {

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
                } catch (LogEmptyException lee) {
                    println("No stream found to dump records.");
                    return -1;
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
    }

    @Override
    protected String getName() {
        return "dlog_tool";
    }

}
