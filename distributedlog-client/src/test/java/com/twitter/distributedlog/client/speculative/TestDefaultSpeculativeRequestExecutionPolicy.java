package com.twitter.distributedlog.client.speculative;

import com.twitter.util.CountDownLatch;
import com.twitter.util.Future;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Test {@link TestDefaultSpeculativeRequestExecutionPolicy}
 */
public class TestDefaultSpeculativeRequestExecutionPolicy {

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testInvalidBackoffMultiplier() throws Exception {
        new DefaultSpeculativeRequestExecutionPolicy(100, 200, -1);
    }

    @Test(timeout = 20000, expected = IllegalArgumentException.class)
    public void testInvalidMaxSpeculativeTimeout() throws Exception {
        new DefaultSpeculativeRequestExecutionPolicy(100, Integer.MAX_VALUE, 2);
    }

    @Test(timeout = 20000)
    public void testSpeculativeRequests() throws Exception {
        DefaultSpeculativeRequestExecutionPolicy policy =
                new DefaultSpeculativeRequestExecutionPolicy(10, 10000, 2);
        SpeculativeRequestExecutor executor = mock(SpeculativeRequestExecutor.class);

        final AtomicInteger callCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(3);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                try {
                    return Future.value(callCount.incrementAndGet() < 3);
                } finally {
                    latch.countDown();
                }
            }
        }).when(executor).issueSpeculativeRequest();

        ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        policy.initiateSpeculativeRequest(executorService, executor);

        latch.await();

        assertEquals(40, policy.getNextSpeculativeRequestTimeout());
    }

    @Test(timeout = 20000)
    public void testSpeculativeRequestsWithMaxTimeout() throws Exception {
        DefaultSpeculativeRequestExecutionPolicy policy =
                new DefaultSpeculativeRequestExecutionPolicy(10, 15, 2);
        SpeculativeRequestExecutor executor = mock(SpeculativeRequestExecutor.class);

        final AtomicInteger callCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(3);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                try {
                    return Future.value(callCount.incrementAndGet() < 3);
                } finally {
                    latch.countDown();
                }
            }
        }).when(executor).issueSpeculativeRequest();

        ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        policy.initiateSpeculativeRequest(executorService, executor);

        latch.await();

        assertEquals(15, policy.getNextSpeculativeRequestTimeout());
    }
}
