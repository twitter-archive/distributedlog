package com.twitter.distributedlog.net;

import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Test Cases of {@link NetUtils}.
 */
public class TestNetUtils {

    static class DNSResolverWithDefaultConstructor implements DNSToSwitchMapping {

        public DNSResolverWithDefaultConstructor() {}

        @Override
        public List<String> resolve(List<String> list) {
            return list;
        }

        @Override
        public void reloadCachedMappings() {
            // no-op
        }
    }

    static class DNSResolverWithUnknownConstructor implements DNSToSwitchMapping {

        public DNSResolverWithUnknownConstructor(int var1, int var2, int var3) {}

        @Override
        public List<String> resolve(List<String> list) {
            return list;
        }

        @Override
        public void reloadCachedMappings() {
            // no-op
        }
    }

    @Test(timeout = 20000)
    public void testGetDNSResolverWithOverrides() throws Exception {
        DNSToSwitchMapping dnsResolver =
                NetUtils.getDNSResolver(DNSResolverForRacks.class, "");
        assertEquals("Should succeed to load " + DNSResolverForRacks.class,
                dnsResolver.getClass(), DNSResolverForRacks.class);
    }

    @Test(timeout = 20000)
    public void testGetDNSResolverWithDefaultConstructor() throws Exception {
        DNSToSwitchMapping dnsResolver =
                NetUtils.getDNSResolver(DNSResolverWithDefaultConstructor.class, "");
        assertEquals("Should succeed to load " + DNSResolverWithDefaultConstructor.class,
                dnsResolver.getClass(), DNSResolverWithDefaultConstructor.class);
    }

    @Test(timeout = 20000, expected = RuntimeException.class)
    public void testGetDNSResolverWithUnknownConstructor() throws Exception {
        NetUtils.getDNSResolver(DNSResolverWithUnknownConstructor.class, "");
    }
}
