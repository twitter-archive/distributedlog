package com.twitter.distributedlog.service.balancer;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestCountBasedStreamChooser {

    @Test(timeout = 60000)
    public void testEmptyStreamDistribution() {
        try {
            new CountBasedStreamChooser(new HashMap<SocketAddress, Set<String>>());
            fail("Should fail constructing stream chooser if the stream distribution is empty");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testMultipleHostsWithEmptyStreams() {
        for (int i = 1; i <= 3; i++) {
            Map<SocketAddress, Set<String>> streamDistribution = new HashMap<SocketAddress, Set<String>>();
            int port = 1000;
            for (int j = 0; j < i; j++) {
                SocketAddress address = new InetSocketAddress("127.0.0.1", port + j);
                streamDistribution.put(address, new HashSet<String>());
            }

            CountBasedStreamChooser chooser = new CountBasedStreamChooser(streamDistribution);
            for (int k = 0; k < i+1; k++) {
                assertNull(chooser.choose());
            }
        }
    }

    @Test(timeout = 60000)
    public void testSingleHostWithStreams() {
        for (int i = 0; i < 3; i++) {
            Map<SocketAddress, Set<String>> streamDistribution = new HashMap<SocketAddress, Set<String>>();

            Set<String> streams = new HashSet<String>();
            for (int j = 0; j < 3; j++) {
                streams.add("SingleHostStream-" + j);
            }

            int port = 1000;
            SocketAddress address = new InetSocketAddress("127.0.0.1", port);
            streamDistribution.put(address, streams);

            for (int k = 1; k <= i; k++) {
                address = new InetSocketAddress("127.0.0.1", port + k);
                streamDistribution.put(address, new HashSet<String>());
            }

            Set<String> choosenStreams = new HashSet<String>();

            CountBasedStreamChooser chooser = new CountBasedStreamChooser(streamDistribution);
            for (int l = 0; l < 3 + i + 1; l++) {
                String s = chooser.choose();
                if (null != s) {
                    choosenStreams.add(s);
                }
            }

            assertEquals(streams.size(), choosenStreams.size());
            assertTrue(Sets.difference(streams, choosenStreams).immutableCopy().isEmpty());
        }
    }

    @Test(timeout = 60000)
    public void testHostsHaveSameNumberStreams() {
        Map<SocketAddress, Set<String>> streamDistribution = new HashMap<SocketAddress, Set<String>>();
        Set<String> allStreams = new HashSet<String>();

        int numHosts = 3;
        int numStreamsPerHost = 3;

        int port = 1000;
        for (int i = 1; i <= numHosts; i++) {
            SocketAddress address = new InetSocketAddress("127.0.0.1", port + i);
            Set<String> streams = new HashSet<String>();

            for (int j = 1; j <= numStreamsPerHost; j++) {
                String streamName = "HostsHaveSameNumberStreams-" + i + "-" + j;
                streams.add(streamName);
                allStreams.add(streamName);
            }

            streamDistribution.put(address, streams);
        }

        Set<String> streamsChoosen = new HashSet<String>();
        CountBasedStreamChooser chooser = new CountBasedStreamChooser(streamDistribution);
        for (int i = 1; i <= numStreamsPerHost; i++) {
            for (int j = 1; j <= numHosts; j++) {
                String s = chooser.choose();
                assertNotNull(s);
                streamsChoosen.add(s);
            }
            for (int j = 0; j < numHosts; j++) {
                assertEquals(numStreamsPerHost - i, chooser.streamsDistribution.get(j).getRight().size());
            }
        }
        assertNull(chooser.choose());
        assertEquals(numHosts * numStreamsPerHost, streamsChoosen.size());
        assertTrue(Sets.difference(allStreams, streamsChoosen).isEmpty());
    }

    @Test(timeout = 60000)
    public void testHostsHaveDifferentNumberStreams() {
        Map<SocketAddress, Set<String>> streamDistribution = new HashMap<SocketAddress, Set<String>>();
        Set<String> allStreams = new HashSet<String>();

        int numHosts = 6;
        int maxStreamsPerHost = 4;

        int port = 1000;
        for (int i = 0; i < numHosts; i++) {
            int group = i / 2;
            int numStreamsThisGroup = maxStreamsPerHost - group;

            SocketAddress address = new InetSocketAddress("127.0.0.1", port + i);
            Set<String> streams = new HashSet<String>();

            for (int j = 1; j <= numStreamsThisGroup; j++) {
                String streamName = "HostsHaveDifferentNumberStreams-" + i + "-" + j;
                streams.add(streamName);
                allStreams.add(streamName);
            }

            streamDistribution.put(address, streams);
        }

        Set<String> streamsChoosen = new HashSet<String>();
        CountBasedStreamChooser chooser = new CountBasedStreamChooser(streamDistribution);

        for (int i = 0; i < allStreams.size(); i++) {
            String s = chooser.choose();
            assertNotNull(s);
            streamsChoosen.add(s);
        }
        assertNull(chooser.choose());
        assertEquals(allStreams.size(), streamsChoosen.size());
        assertTrue(Sets.difference(allStreams, streamsChoosen).isEmpty());
    }

    @Test(timeout = 60000)
    public void testLimitedStreamChooser() {
        Map<SocketAddress, Set<String>> streamDistribution = new HashMap<SocketAddress, Set<String>>();

        Set<String> streams = new HashSet<String>();
        for (int j = 0; j < 10; j++) {
            streams.add("SingleHostStream-" + j);
        }

        int port = 1000;
        SocketAddress address = new InetSocketAddress("127.0.0.1", port);
        streamDistribution.put(address, streams);

        Set<String> choosenStreams = new HashSet<String>();

        CountBasedStreamChooser underlying = new CountBasedStreamChooser(streamDistribution);
        LimitedStreamChooser chooser = LimitedStreamChooser.of(underlying, 1);
        for (int l = 0; l < 10; l++) {
            String s = chooser.choose();
            if (null != s) {
                choosenStreams.add(s);
            }
        }

        assertEquals(1, choosenStreams.size());
    }
}
