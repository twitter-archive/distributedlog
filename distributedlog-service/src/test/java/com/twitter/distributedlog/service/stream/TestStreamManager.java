package com.twitter.distributedlog.service.stream;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.service.config.StreamConfigProvider;
import com.twitter.distributedlog.service.streamset.Partition;
import com.twitter.distributedlog.service.streamset.StreamPartitionConverter;
import com.twitter.util.Await;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test Case for StreamManager.
 */
public class TestStreamManager {

    @Rule
    public TestName testName = new TestName();

    ScheduledExecutorService mockExecutorService = mock(ScheduledExecutorService.class);

    @Test(timeout = 60000)
    public void testCollectionMethods() throws Exception {
        Stream mockStream = mock(Stream.class);
        when(mockStream.getStreamName()).thenReturn("stream1");
        when(mockStream.getPartition()).thenReturn(new Partition("stream1", 0));
        StreamFactory mockStreamFactory = mock(StreamFactory.class);
        StreamPartitionConverter mockPartitionConverter = mock(StreamPartitionConverter.class);
        StreamConfigProvider mockStreamConfigProvider = mock(StreamConfigProvider.class);
        when(mockStreamFactory.create(
                (String)any(),
                (DynamicDistributedLogConfiguration) any(),
                (StreamManager)any())).thenReturn(mockStream);
        StreamManager streamManager = new StreamManagerImpl(
                "",
                new DistributedLogConfiguration(),
                mockExecutorService,
                mockStreamFactory,
                mockPartitionConverter,
                mockStreamConfigProvider,
                mock(DistributedLogNamespace.class));

        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyAcquired(mockStream);
        assertTrue(streamManager.isAcquired("stream1"));
        assertEquals(1, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyReleased(mockStream);
        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyAcquired(mockStream);
        assertTrue(streamManager.isAcquired("stream1"));
        assertEquals(1, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyAcquired(mockStream);
        assertTrue(streamManager.isAcquired("stream1"));
        assertEquals(1, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyReleased(mockStream);
        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());

        streamManager.notifyReleased(mockStream);
        assertFalse(streamManager.isAcquired("stream1"));
        assertEquals(0, streamManager.numAcquired());
        assertEquals(0, streamManager.numCached());
    }

    @Test
    public void testCreateStream() throws Exception {
        Stream mockStream = mock(Stream.class);
        final String streamName = "stream1";
        when(mockStream.getStreamName()).thenReturn(streamName);
        StreamFactory mockStreamFactory = mock(StreamFactory.class);
        StreamPartitionConverter mockPartitionConverter = mock(StreamPartitionConverter.class);
        StreamConfigProvider mockStreamConfigProvider = mock(StreamConfigProvider.class);
        when(mockStreamFactory.create((String)any(), (DynamicDistributedLogConfiguration)any(), (StreamManager)any())).thenReturn(mockStream);
        DistributedLogNamespace dlNamespace = mock(DistributedLogNamespace.class);
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

        StreamManager streamManager = new StreamManagerImpl(
                "",
                new DistributedLogConfiguration(),
                executorService,
                mockStreamFactory,
                mockPartitionConverter,
                mockStreamConfigProvider,
                dlNamespace);

        assertTrue(Await.ready(streamManager.createStreamAsync(streamName)).isReturn());
        verify(dlNamespace).createLog(streamName);
    }
}
