/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
