package com.twitter.distributedlog.v2.util;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestConfUtils {

    @Test
    public void testLoadConfiguration() {
        Configuration conf1 = new CompositeConfiguration();
        conf1.setProperty("key1", "value1");
        conf1.setProperty("key2", "value2");
        conf1.setProperty("key3", "value3");

        Configuration conf2 = new CompositeConfiguration();
        conf2.setProperty("bkc.key1", "bkc.value1");
        conf2.setProperty("bkc.key4", "bkc.value4");

        assertEquals("value1", conf1.getString("key1"));
        assertEquals("value2", conf1.getString("key2"));
        assertEquals("value3", conf1.getString("key3"));
        assertEquals(null, conf1.getString("key4"));

        ConfUtils.loadConfiguration(conf1, conf2, "bkc.");

        assertEquals("bkc.value1", conf1.getString("key1"));
        assertEquals("value2", conf1.getString("key2"));
        assertEquals("value3", conf1.getString("key3"));
        assertEquals("bkc.value4", conf1.getString("key4"));
        assertEquals(null, conf1.getString("bkc.key1"));
        assertEquals(null, conf1.getString("bkc.key4"));
    }
}
