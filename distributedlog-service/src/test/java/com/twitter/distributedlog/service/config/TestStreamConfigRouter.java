package com.twitter.distributedlog.service.config;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestStreamConfigRouter {
    static final Logger LOG = LoggerFactory.getLogger(TestStreamConfigRouter.class);

    @Test
    public void testIdentityConfigRouter() throws Exception {
        StreamConfigRouter router = new IdentityConfigRouter();
        assertIdentity("test1", router);
        assertIdentity(null, router);
    }

    @Test
    public void testEventbusConfigRouterWithNormalStream() throws Exception {
        StreamConfigRouter router = new EventbusPartitionConfigRouter();
        assertEquals("test1", router.getConfig("eventbus_test1_000001"));
    }

    @Test
    public void testEventbusConfigRouterWithDarkStream() throws Exception {
        StreamConfigRouter router = new EventbusPartitionConfigRouter();
        assertEquals("test1", router.getConfig("__dark_eventbus_test1_000001"));
    }

    private void assertIdentity(String streamName, StreamConfigRouter router) {
        assertEquals(streamName, router.getConfig(streamName));
    }

    @Test
    public void testEventbusConfigRouterWithNonEventBusStream() throws Exception {
        StreamConfigRouter router = new EventbusPartitionConfigRouter();
        assertIdentity("test1", router);
        assertIdentity("test1_000001", router);
        assertIdentity("eventbus_test1_00001", router);
        assertIdentity("blah_test1_000001", router);
        assertIdentity("_dark_eventbus_test1_000001", router);
        assertIdentity("eventbus_test1_00a001", router);
        assertIdentity("eventbus__00a001", router);
    }
}
