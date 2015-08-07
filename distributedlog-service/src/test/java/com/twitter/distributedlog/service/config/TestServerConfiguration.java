package com.twitter.distributedlog.service.config;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestServerConfiguration {

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testUnassignedShardId() {
        new ServerConfiguration().validate();
    }

    @Test(timeout = 60000)
    public void testAssignedShardId() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerShardId(100);
        conf.validate();
        assertEquals(100, conf.getServerShardId());
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testInvalidServerThreads() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerShardId(100);
        conf.setServerThreads(-1);
        conf.validate();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testInvalidDlsnVersion() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerShardId(100);
        conf.setDlsnVersion((byte) 9999);
        conf.validate();
    }

}
