package com.twitter.distributedlog.namespace;

import com.twitter.distributedlog.BKDistributedLogNamespace;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestDistributedLogBase;
import org.junit.Test;

import java.net.URI;

import static com.twitter.distributedlog.LocalDLMEmulator.DLOG_NAMESPACE;
import static org.junit.Assert.assertTrue;

/**
 * Test Namespace Builder
 */
public class TestDistributedLogNamespaceBuilder extends TestDistributedLogBase {

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testEmptyBuilder() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder().build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testMissingUri() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testMissingSchemeInUri() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("/test"))
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testInvalidSchemeInUri() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("dist://invalid/scheme/in/uri"))
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testInvalidSchemeCorrectBackendInUri() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("dist-bk://invalid/scheme/in/uri"))
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testUnknownBackendInUri() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("distributedlog-unknown://invalid/scheme/in/uri"))
                .build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testNullStatsLogger() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("distributedlog-bk://localhost/distributedlog"))
                .statsLogger(null)
                .build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testNullClientId() throws Exception {
        DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("distributedlog-bk://localhost/distributedlog"))
                .clientId(null)
                .build();
    }

    @Test(timeout = 60000)
    public void testBuildBKDistributedLogNamespace() throws Exception {
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("distributedlog-bk://" + zkServers + DLOG_NAMESPACE + "/bknamespace"))
                .build();
        try {
            assertTrue("distributedlog-bk:// should build bookkeeper based distributedlog namespace",
                    namespace instanceof BKDistributedLogNamespace);
        } finally {
            namespace.close();
        }
    }

    @Test(timeout = 60000)
    public void testBuildWhenMissingBackendInUri() throws Exception {
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(new DistributedLogConfiguration())
                .uri(new URI("distributedlog://" + zkServers + DLOG_NAMESPACE + "/defaultnamespace"))
                .build();
        try {
            assertTrue("distributedlog:// should build bookkeeper based distributedlog namespace",
                    namespace instanceof BKDistributedLogNamespace);
        } finally {
            namespace.close();
        }
    }
}
