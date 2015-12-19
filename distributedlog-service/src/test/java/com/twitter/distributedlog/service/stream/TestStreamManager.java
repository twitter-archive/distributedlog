package com.twitter.distributedlog.service.stream;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test Case for StreamManager.
 */
public class TestStreamManager {
    static final Logger logger = LoggerFactory.getLogger(TestStreamManager.class);

    @Rule
    public TestName testName = new TestName();

    ScheduledExecutorService mockExecutorService = mock(ScheduledExecutorService.class);

    @Test(timeout = 60000)
    public void testCollectionMethods() throws Exception {
        Stream mockStream = mock(Stream.class);
        when(mockStream.getStreamName()).thenReturn("stream1");
        StreamFactory mockStreamFactory = mock(StreamFactory.class);
        when(mockStreamFactory.create((String)any(), (StreamManager)any())).thenReturn(mockStream);
        StreamManager streamManager = new StreamManagerImpl("", mockExecutorService, mockStreamFactory);

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
}
