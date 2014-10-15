package com.twitter.distributedlog.admin;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.DistributedLogManagerFactory;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.LogSegmentLedgerMetadata;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import com.twitter.distributedlog.metadata.DryrunZkMetadataUpdater;
import com.twitter.distributedlog.metadata.ZkMetadataUpdater;
import com.twitter.distributedlog.util.SchedulerUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestDLCK extends TestDistributedLogBase {

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setEnableLedgerAllocatorPool(true).setLedgerAllocatorPoolName("test");

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = ZooKeeperClientBuilder
            .newBuilder()
            .uri(DLMTestUtil.createDLMURI("/"))
            .zkAclId(null)
            .sessionTimeoutMs(10000).build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    static Map<Long, LogSegmentLedgerMetadata> getLogSegments(DistributedLogManager dlm) throws Exception {
        Map<Long, LogSegmentLedgerMetadata> logSegmentMap =
                new HashMap<Long, LogSegmentLedgerMetadata>();
        List<LogSegmentLedgerMetadata> segments = dlm.getLogSegments();
        for (LogSegmentLedgerMetadata segment : segments) {
            logSegmentMap.put(segment.getLedgerSequenceNumber(), segment);
        }
        return logSegmentMap;
    }

    static void verifyLogSegment(Map<Long, LogSegmentLedgerMetadata> segments,
                                 DLSN lastDLSN, long ledgerSequenceNumber,
                                 int recordCount, long lastTxId) {
        LogSegmentLedgerMetadata segment = segments.get(ledgerSequenceNumber);
        assertNotNull(segment);
        assertEquals(lastDLSN, segment.getLastDLSN());
        assertEquals(recordCount, segment.getRecordCount());
        assertEquals(lastTxId, segment.getLastTxId());
    }

    @Test(timeout = 60000)
    public void testCheckAndRepairDLNamespace() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        conf.setImmediateFlushEnabled(true);
        conf.setOutputBufferSize(0);
        URI uri = DLMTestUtil.createDLMURI("/check-and-repair-dl-namespace");
        zkc.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        DistributedLogManagerFactory factory = new DistributedLogManagerFactory(confLocal, uri);
        ExecutorService executorService = Executors.newCachedThreadPool();

        String streamName = "check-and-repair-dl-namespace";

        // Create completed log segments
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, conf, 1L, 1L, 10, false);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, conf, 2L, 11L, 10, true);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, conf, 3L, 21L, 10, false);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, conf, 4L, 31L, 10, true);

        // dryrun
        BookKeeperClient bkc = factory.getReaderBKC();
        DistributedLogAdmin.checkAndRepairDLNamespace(uri, factory,
                new DryrunZkMetadataUpdater(factory.getSharedWriterZKCForDL()),
                executorService, bkc, confLocal.getBKDigestPW(), false, false);

        Map<Long, LogSegmentLedgerMetadata> segments = getLogSegments(dlm);
        verifyLogSegment(segments, new DLSN(1L, 18L, 0L), 1L, 10, 10L);
        verifyLogSegment(segments, new DLSN(2L, 16L, 0L), 2L, 9, 19L);
        verifyLogSegment(segments, new DLSN(3L, 18L, 0L), 3L, 10, 30L);
        verifyLogSegment(segments, new DLSN(4L, 16L, 0L), 4L, 9, 39L);

        // check and repair
        bkc = factory.getReaderBKC();
        DistributedLogAdmin.checkAndRepairDLNamespace(uri, factory,
                ZkMetadataUpdater.createMetadataUpdater(factory.getSharedWriterZKCForDL()),
                executorService, bkc, confLocal.getBKDigestPW(), false, false);

        segments = getLogSegments(dlm);
        verifyLogSegment(segments, new DLSN(1L, 18L, 0L), 1L, 10, 10L);
        verifyLogSegment(segments, new DLSN(2L, 18L, 0L), 2L, 10, 20L);
        verifyLogSegment(segments, new DLSN(3L, 18L, 0L), 3L, 10, 30L);
        verifyLogSegment(segments, new DLSN(4L, 18L, 0L), 4L, 10, 40L);

        dlm.close();
        SchedulerUtils.shutdownScheduler(executorService, 5, TimeUnit.MINUTES);
        factory.close();
    }

}
