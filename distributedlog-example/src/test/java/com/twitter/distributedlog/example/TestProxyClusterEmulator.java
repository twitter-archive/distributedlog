package com.twitter.distributedlog.example;

import java.net.BindException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestProxyClusterEmulator {
    static final Logger logger = LoggerFactory.getLogger(TestProxyClusterEmulator.class);

    @Test(timeout = 60000)
    public void testStartup() throws Exception {
        final ProxyClusterEmulator emulator = new ProxyClusterEmulator(new String[] {"-port", "8000"});
        final AtomicBoolean failed = new AtomicBoolean(false);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    started.countDown();
                    emulator.start();
                } catch (BindException ex) {
                } catch (InterruptedException ex) {
                } catch (Exception ex) {
                    failed.set(true);
                } finally {
                    finished.countDown();
                }
            }
        });
        thread.start();
        started.await();
        Thread.sleep(1000);
        thread.interrupt();
        finished.await();
        emulator.stop();
        assert(!failed.get());
    }
}
