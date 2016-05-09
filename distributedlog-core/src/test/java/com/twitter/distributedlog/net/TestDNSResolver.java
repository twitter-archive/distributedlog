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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestDNSResolver {

    private static final String host1 = "r1-w1rack1-1111-2222.distributedlog.io";
    private static final String host2 = "r2-w2rack2-3333-4444.distributedlog.io";

    @Test(timeout = 20000)
    public void testDNSResolverForRacks() {
        DNSResolver dnsResolver = new DNSResolverForRacks("");

        List<String> ipList = new ArrayList<String>();
        ipList.add("192.0.0.1");
        List<String> racks = dnsResolver.resolve(ipList);
        assertEquals(DNSResolverForRacks.DEFAULT_RACK, racks.get(0));

        List<String> unknownList = new ArrayList<String>();
        unknownList.add("unknown");
        racks = dnsResolver.resolve(unknownList);
        assertEquals(DNSResolverForRacks.DEFAULT_RACK, racks.get(0));

        List<String> r1List = new ArrayList<String>();
        r1List.add(host1);
        racks = dnsResolver.resolve(r1List);
        assertEquals("/r1/w1rack1", racks.get(0));

        List<String> r2List = new ArrayList<String>();
        r2List.add(host2);
        racks = dnsResolver.resolve(r2List);
        assertEquals("/r2/w2rack2", racks.get(0));
    }

    @Test(timeout = 20000)
    public void testDNSResolverForRows() {
        DNSResolver dnsResolver = new DNSResolverForRows("");

        List<String> ipList = new ArrayList<String>();
        ipList.add("192.0.0.1");
        List<String> rows = dnsResolver.resolve(ipList);
        assertEquals(DNSResolverForRows.DEFAULT_ROW, rows.get(0));

        List<String> unknownList = new ArrayList<String>();
        unknownList.add("unknown");
        rows = dnsResolver.resolve(unknownList);
        assertEquals(DNSResolverForRows.DEFAULT_ROW, rows.get(0));

        List<String> r1List = new ArrayList<String>();
        r1List.add(host1);
        rows = dnsResolver.resolve(r1List);
        assertEquals("/r1/w1", rows.get(0));

        List<String> r2List = new ArrayList<String>();
        r2List.add(host2);
        rows = dnsResolver.resolve(r2List);
        assertEquals("/r2/w2", rows.get(0));
    }

    @Test(timeout = 20000)
    public void testDNSResolverOverrides() {
        DNSResolver dnsResolver = new DNSResolverForRacks("r1-w1rack1-1111-2222:r3;r2-w2rack2-3333-4444:r3");

        List<String> r1List = new ArrayList<String>();
        r1List.add(host1);
        List<String> racks = dnsResolver.resolve(r1List);
        assertEquals("/r3/w1rack1", racks.get(0));

        List<String> r2List = new ArrayList<String>();
        r2List.add(host2);
        racks = dnsResolver.resolve(r2List);
        assertEquals("/r3/w2rack2", racks.get(0));
    }
}
