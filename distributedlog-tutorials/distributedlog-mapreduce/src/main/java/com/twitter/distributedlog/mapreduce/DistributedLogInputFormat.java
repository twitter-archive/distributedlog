package com.twitter.distributedlog.mapreduce;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.BKDistributedLogNamespace;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.LogSegmentMetadata;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAccessor;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * InputFormat to read data from a distributedlog stream.
 */
public class DistributedLogInputFormat
        extends InputFormat<DLSN, LogRecordWithDLSN> implements Configurable {

    private static final String DL_URI = "distributedlog.uri";
    private static final String DL_STREAM = "distributedlog.stream";

    protected Configuration conf;
    protected DistributedLogConfiguration dlConf;
    protected URI dlUri;
    protected BKDistributedLogNamespace namespace;
    protected String streamName;
    protected DistributedLogManager dlm;

    /** {@inheritDoc} */
    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        dlConf = new DistributedLogConfiguration();
        dlUri = URI.create(configuration.get(DL_URI, ""));
        streamName = configuration.get(DL_STREAM, "");
        try {
            namespace = BKDistributedLogNamespace.newBuilder()
                    .conf(dlConf)
                    .uri(dlUri)
                    .build();
            dlm = namespace.openLog(streamName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        List<InputSplit> inputSplits = Lists.newArrayListWithCapacity(segments.size());
        BookKeeper bk = namespace.getReaderBKC().get();
        LedgerManager lm = BookKeeperAccessor.getLedgerManager(bk);
        final AtomicInteger rcHolder = new AtomicInteger(0);
        final AtomicReference<LedgerMetadata> metadataHolder = new AtomicReference<LedgerMetadata>(null);
        for (LogSegmentMetadata segment : segments) {
            final CountDownLatch latch = new CountDownLatch(1);
            lm.readLedgerMetadata(segment.getLedgerId(),
                    new BookkeeperInternalCallbacks.GenericCallback<LedgerMetadata>() {
                @Override
                public void operationComplete(int rc, LedgerMetadata ledgerMetadata) {
                    metadataHolder.set(ledgerMetadata);
                    rcHolder.set(rc);
                    latch.countDown();
                }
            });
            latch.await();
            if (BKException.Code.OK != rcHolder.get()) {
                throw new IOException("Faild to get log segment metadata for " + segment + " : "
                        + BKException.getMessage(rcHolder.get()));
            }
            inputSplits.add(new LogSegmentSplit(segment, metadataHolder.get()));
        }
        return inputSplits;
    }

    @Override
    public RecordReader<DLSN, LogRecordWithDLSN> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new LogSegmentReader(
                streamName,
                dlConf,
                namespace.getReaderBKC().get(),
                (LogSegmentSplit) inputSplit);
    }
}
