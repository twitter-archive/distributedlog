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

    private void assertIdentity(String streamName, StreamConfigRouter router) {
        assertEquals(streamName, router.getConfig(streamName));
    }
}
