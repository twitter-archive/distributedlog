package com.twitter.distributedlog;

import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.*;

public class TestAppendOnlyStreamWriter {

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10);
    private ZooKeeper zkc;
    private static LocalDLMEmulator bkutil;
    private static ZooKeeperServerShim zks;
    static int numBookies = 3;

    @BeforeClass
    public static void setupCluster() throws Exception {
        zks = LocalBookKeeper.runZookeeper(1000, 7000);
        bkutil = new LocalDLMEmulator(numBookies, "127.0.0.1", 7000);
        bkutil.start();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
    }

    @Before
    public void setup() throws Exception {
        zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", 7000);
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test
    public void basicReadAndWriteBehavior() throws Exception {
        String name = "distrlog-append-only-streams-basic";
        DistributedLogManager dlmwrite = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 51).getBytes();

        long txid = 1;
        AppendOnlyStreamWriter writer = dlmwrite.getAppendOnlyStreamWriter();
        writer.write(DLMTestUtil.repeatString("abc", 11).getBytes());
        writer.write(DLMTestUtil.repeatString("abc", 40).getBytes());
        writer.force(false);
        writer.close();
        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();

        byte[] bytesIn = new byte[byteStream.length];
        int read = reader.read(bytesIn, 0, 23);
        assertEquals(23, read);
        read = reader.read(bytesIn, 23, 31);
        assertEquals(read, 31);
        byte[] bytesInTemp = new byte[byteStream.length];
        read = reader.read(bytesInTemp, 0, byteStream.length);
        assertEquals(read, byteStream.length - 23 - 31);
        read = new ByteArrayInputStream(bytesInTemp).read(bytesIn, 23 + 31, byteStream.length - 23 - 31);
        assertEquals(read, byteStream.length - 23 - 31);
        assertArrayEquals(bytesIn, byteStream);
        reader.close();
        dlmreader.close();
        dlmwrite.close();
    }
    
    @Test
    public void writeFutureDoesNotCompleteUntilWritePersisted() throws Exception {
        String name = "distrlog-append-only-streams-async-success";
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setPeriodicFlushFrequencyMilliSeconds(Integer.MAX_VALUE);
        conf.setImmediateFlushEnabled(false);

        DistributedLogManager dlmwriter = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 51).getBytes();
        
        // Can't reliably test the future is not completed until fsync is called, since writer.force may just 
        // happen very quickly. But we can test that the mechanics of the future write and api are basically 
        // correct.   
        AppendOnlyStreamWriter writer = dlmwriter.getAppendOnlyStreamWriter();
        Future<DLSN> dlsnFuture = writer.write(DLMTestUtil.repeatString("abc", 11).getBytes());
        
        // This won't impact flakiness, it just increases the probability that the writer will complete the 
        // future if something's wrong.
        Thread.sleep(100);
        assertFalse(dlsnFuture.isDefined());
        writer.force(false);
        // Must not throw.
        Await.result(dlsnFuture, Duration.fromSeconds(5));
        writer.close();
        dlmwriter.close();

        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();
        byte[] bytesIn = new byte[byteStream.length];
        int read = reader.read(bytesIn, 0, 31);
        assertEquals(31, read);
        reader.close();
        dlmreader.close();
    } 

    @Test
    public void positionUpdatesOnlyAfterWriteCompletion() throws Exception {
        String name = "distrlog-append-only-streams-async-fsync";
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.setPeriodicFlushFrequencyMilliSeconds(10*1000);
        conf.setImmediateFlushEnabled(false);

        DistributedLogManager dlmwriter = DLMTestUtil.createNewDLM(conf, name);
        DistributedLogManager dlmreader = DLMTestUtil.createNewDLM(conf, name);
        byte[] byteStream = DLMTestUtil.repeatString("abc", 11).getBytes();
        
        // Can't reliably test the future is not completed until fsync is called, since writer.force may just 
        // happen very quickly. But we can test that the mechanics of the future write and api are basically 
        // correct.   
        AppendOnlyStreamWriter writer = dlmwriter.getAppendOnlyStreamWriter();
        Future<DLSN> dlsnFuture = writer.write(byteStream);
        Thread.sleep(100);

        // Write hasn't been persisted, position better not be updated.
        assertFalse(dlsnFuture.isDefined());
        assertEquals(0, writer.position());
        writer.force(false);
        // Position guaranteed to be accurate after writer.force().
        assertEquals(byteStream.length, writer.position());
        
        // Close writer.
        writer.close();
        dlmwriter.close();

        // Make sure we can read it.
        AppendOnlyStreamReader reader = dlmreader.getAppendOnlyStreamReader();
        byte[] bytesIn = new byte[byteStream.length];
        int read = reader.read(bytesIn, 0, byteStream.length);
        assertEquals(byteStream.length, read);
        assertEquals(byteStream.length, reader.position());
        reader.close();
        dlmreader.close();
    } 
}