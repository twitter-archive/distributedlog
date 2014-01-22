package com.twitter.distributedlog.tools;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.BookKeeperClientBuilder;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LogEmptyException;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
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
import java.net.MalformedURLException;
import java.net.URI;
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

    class DumpCommand extends PerStreamCommand {

        PartitionId partitionId = null;
        boolean printHex = false;
        Long fromTxnId = null;
        int count = 100;

        DumpCommand() {
            super("dump", "dump records of a given stream");
            options.addOption("p", "partition", true, "Partition of given stream");
            options.addOption("x", "hex", false, "Print record in hex format");
            options.addOption("o", "offset", true, "Txn ID to start dumping.");
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
        }

        @Override
        protected int runCmd() throws Exception {
            DistributedLogManager dlm = DistributedLogManagerFactory.createDistributedLogManager(
                    getStreamName(), getConf(), getUri());
            try {
                LogReader reader;
                try {
                    if (null == partitionId) {
                        reader = dlm.getInputStream(fromTxnId);
                    } else {
                        reader = dlm.getInputStream(partitionId, fromTxnId);
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
            println("Record (txn = " + record.getTransactionId() + ", bytes = "
                    + record.getPayload().length + ")");
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
        addCommand(new ListCommand());
        addCommand(new DumpCommand());
        addCommand(new ShowCommand());
        addCommand(new DeleteCommand());
        addCommand(new DeleteAllocatorPoolCommand());
    }

    @Override
    protected String getName() {
        return "dlog_tool";
    }

}
