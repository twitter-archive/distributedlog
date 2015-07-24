package com.twitter.distributedlog.example;

import java.net.BindException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProxyClusterEmulator {
    static final Logger logger = LoggerFactory.getLogger(TestProxyClusterEmulator.class);

    @Test(timeout = 60000)
    public void testStartup() throws Exception {
        ProxyClusterEmulator emulator = new ProxyClusterEmulator(new String[] {"8000"});
        try {
            emulator.start();
        } catch (BindException ex) {
        }
        emulator.stop();
    }
}
