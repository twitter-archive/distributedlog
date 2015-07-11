package com.twitter.distributedlog.config;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestConcurrentBaseConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(TestConcurrentBaseConfiguration.class);

    @Test
    public void testBasicOperations() throws Exception {
        ConcurrentBaseConfiguration conf = new ConcurrentBaseConfiguration();
        conf.setProperty("prop1", "1");
        assertEquals(1, conf.getInt("prop1"));
        conf.setProperty("prop1", "2");
        assertEquals(2, conf.getInt("prop1"));
        conf.clearProperty("prop1");
        assertEquals(null, conf.getInteger("prop1", null));
        conf.setProperty("prop1", "1");
        conf.setProperty("prop2", "2");
        assertEquals(1, conf.getInt("prop1"));
        assertEquals(2, conf.getInt("prop2"));
        conf.clearProperty("prop1");
        assertEquals(null, conf.getInteger("prop1", null));
        assertEquals(2, conf.getInt("prop2"));
    }
}
