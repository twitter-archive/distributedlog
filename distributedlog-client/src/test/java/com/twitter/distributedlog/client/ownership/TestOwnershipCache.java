package com.twitter.distributedlog.client.ownership;

import com.twitter.finagle.stats.NullStatsReceiver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test Case for Ownership Cache
 */
public class TestOwnershipCache {

    @Rule
    public TestName runtime = new TestName();

    private static OwnershipCache createOwnershipCache() {
        return new OwnershipCache(NullStatsReceiver.get(), NullStatsReceiver.get());
    }

    private static SocketAddress createSocketAddress(int port) {
        return new InetSocketAddress("127.0.0.1", port);
    }

    @Test(timeout = 60000)
    public void testUpdateOwner() {
        OwnershipCache cache = createOwnershipCache();
        SocketAddress addr = createSocketAddress(1000);
        String stream = runtime.getMethodName();

        assertTrue("Should successfully update owner if no owner exists before",
                cache.updateOwner(stream, addr));
        assertEquals("Owner should be " + addr + " for stream " + stream,
                addr, cache.getOwner(stream));
        assertTrue("Should successfully update owner if old owner is same",
                cache.updateOwner(stream, addr));
        assertEquals("Owner should be " + addr + " for stream " + stream,
                addr, cache.getOwner(stream));
    }

    @Test(timeout = 60000)
    public void testRemoveOwnerFromStream() {
        OwnershipCache cache = createOwnershipCache();
        int initialPort = 2000;
        int numProxies = 2;
        int numStreamsPerProxy = 2;
        for (int i = 0; i < numProxies; i++) {
            SocketAddress addr = createSocketAddress(initialPort + i);
            for (int j = 0; j < numStreamsPerProxy; j++) {
                String stream = runtime.getMethodName() + "_" + i + "_" + j;
                cache.updateOwner(stream, addr);
            }
        }
        Map<String, SocketAddress> ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should be " + (numProxies * numStreamsPerProxy) + " entries in cache",
                numProxies * numStreamsPerProxy, ownershipMap.size());
        Map<SocketAddress, Set<String>> ownershipDistribution = cache.getStreamOwnershipDistribution();
        assertEquals("There should be " + numProxies + " proxies cached",
                numProxies, ownershipDistribution.size());

        String stream = runtime.getMethodName() + "_0_0";
        SocketAddress owner = createSocketAddress(initialPort);

        // remove non-existent mapping won't change anything
        SocketAddress nonExistentAddr = createSocketAddress(initialPort + 999);
        cache.removeOwnerFromStream(stream, nonExistentAddr, "remove-non-existent-addr");
        assertEquals("Owner " + owner + " should not be removed",
                owner, cache.getOwner(stream));
        ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should be " + (numProxies * numStreamsPerProxy) + " entries in cache",
                numProxies * numStreamsPerProxy, ownershipMap.size());

        // remove existent mapping should remove ownership mapping
        cache.removeOwnerFromStream(stream, owner, "remove-owner");
        assertNull("Owner " + owner + " should be removed", cache.getOwner(stream));
        ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should be " + (numProxies * numStreamsPerProxy - 1) + " entries left in cache",
                numProxies * numStreamsPerProxy - 1, ownershipMap.size());
        ownershipDistribution = cache.getStreamOwnershipDistribution();
        assertEquals("There should still be " + numProxies + " proxies cached",
                numProxies, ownershipDistribution.size());
        Set<String> ownedStreams = ownershipDistribution.get(owner);
        assertEquals("There should be only " + (numStreamsPerProxy - 1) + " streams owned for " + owner,
                numStreamsPerProxy - 1, ownedStreams.size());
        assertFalse("Stream " + stream + " should not be owned by " + owner,
                ownedStreams.contains(stream));
    }

    @Test(timeout = 60000)
    public void testRemoveAllStreamsFromOwner() {
        OwnershipCache cache = createOwnershipCache();
        int initialPort = 2000;
        int numProxies = 2;
        int numStreamsPerProxy = 2;
        for (int i = 0; i < numProxies; i++) {
            SocketAddress addr = createSocketAddress(initialPort + i);
            for (int j = 0; j < numStreamsPerProxy; j++) {
                String stream = runtime.getMethodName() + "_" + i + "_" + j;
                cache.updateOwner(stream, addr);
            }
        }
        Map<String, SocketAddress> ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should be " + (numProxies * numStreamsPerProxy) + " entries in cache",
                numProxies * numStreamsPerProxy, ownershipMap.size());
        Map<SocketAddress, Set<String>> ownershipDistribution = cache.getStreamOwnershipDistribution();
        assertEquals("There should be " + numProxies + " proxies cached",
                numProxies, ownershipDistribution.size());

        SocketAddress owner = createSocketAddress(initialPort);

        // remove non-existent host won't change anything
        SocketAddress nonExistentAddr = createSocketAddress(initialPort + 999);
        cache.removeAllStreamsFromOwner(nonExistentAddr);
        ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should still be " + (numProxies * numStreamsPerProxy) + " entries in cache",
                numProxies * numStreamsPerProxy, ownershipMap.size());
        ownershipDistribution = cache.getStreamOwnershipDistribution();
        assertEquals("There should still be " + numProxies + " proxies cached",
                numProxies, ownershipDistribution.size());

        // remove existent host should remove ownership mapping
        cache.removeAllStreamsFromOwner(owner);
        ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should be " + ((numProxies - 1) * numStreamsPerProxy) + " entries left in cache",
                (numProxies - 1) * numStreamsPerProxy, ownershipMap.size());
        ownershipDistribution = cache.getStreamOwnershipDistribution();
        assertEquals("There should be " + (numProxies - 1) + " proxies cached",
                numProxies - 1, ownershipDistribution.size());
        assertFalse("Host " + owner + " should not be cached",
                ownershipDistribution.containsKey(owner));
    }

    @Test(timeout = 60000)
    public void testReplaceOwner() {
        OwnershipCache cache = createOwnershipCache();
        int initialPort = 2000;
        int numProxies = 2;
        int numStreamsPerProxy = 2;
        for (int i = 0; i < numProxies; i++) {
            SocketAddress addr = createSocketAddress(initialPort + i);
            for (int j = 0; j < numStreamsPerProxy; j++) {
                String stream = runtime.getMethodName() + "_" + i + "_" + j;
                cache.updateOwner(stream, addr);
            }
        }
        Map<String, SocketAddress> ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should be " + (numProxies * numStreamsPerProxy) + " entries in cache",
                numProxies * numStreamsPerProxy, ownershipMap.size());
        Map<SocketAddress, Set<String>> ownershipDistribution = cache.getStreamOwnershipDistribution();
        assertEquals("There should be " + numProxies + " proxies cached",
                numProxies, ownershipDistribution.size());

        String stream = runtime.getMethodName() + "_0_0";
        SocketAddress oldOwner = createSocketAddress(initialPort);
        SocketAddress newOwner = createSocketAddress(initialPort + 999);

        cache.updateOwner(stream, newOwner);
        assertEquals("Owner of " + stream + " should be changed from " + oldOwner + " to " + newOwner,
                newOwner, cache.getOwner(stream));
        ownershipMap = cache.getStreamOwnerMapping();
        assertEquals("There should be " + (numProxies * numStreamsPerProxy) + " entries in cache",
                numProxies * numStreamsPerProxy, ownershipMap.size());
        assertEquals("Owner of " + stream + " should be " + newOwner,
                newOwner, ownershipMap.get(stream));
        ownershipDistribution = cache.getStreamOwnershipDistribution();
        assertEquals("There should be " + (numProxies + 1) + " proxies cached",
                numProxies + 1, ownershipDistribution.size());
        Set<String> oldOwnedStreams = ownershipDistribution.get(oldOwner);
        assertEquals("There should be only " + (numStreamsPerProxy - 1) + " streams owned by " + oldOwner,
                numStreamsPerProxy - 1, oldOwnedStreams.size());
        assertFalse("Stream " + stream + " should not be owned by " + oldOwner,
                oldOwnedStreams.contains(stream));
        Set<String> newOwnedStreams = ownershipDistribution.get(newOwner);
        assertEquals("There should be only " + (numStreamsPerProxy - 1) + " streams owned by " + newOwner,
                1, newOwnedStreams.size());
        assertTrue("Stream " + stream + " should be owned by " + newOwner,
                newOwnedStreams.contains(stream));
    }
}
