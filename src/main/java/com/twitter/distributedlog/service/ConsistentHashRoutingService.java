package com.twitter.distributedlog.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.util.Pair;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRoutingService extends ServerSetRoutingService {

    public static ConsistentHashRoutingService of(ServerSet serverSet, int numReplicas) {
        return new ConsistentHashRoutingService(serverSet, numReplicas);
    }

    static class ConsistentHash {
        private final HashFunction hashFunction;
        private final int numOfReplicas;
        private final SortedMap<Long, SocketAddress> circle;

        ConsistentHash(HashFunction hashFunction, int numOfReplicas) {
            this.hashFunction = hashFunction;
            this.numOfReplicas = numOfReplicas;
            this.circle = new TreeMap<Long, SocketAddress>();
        }

        private String replicaName(int shardId, int replica) {
            return String.format("shard-%d-%d", shardId, replica);
        }

        private Long replicaHash(int shardId, int replica) {
            return hashFunction.hashString(replicaName(shardId, replica)).asLong();
        }

        public synchronized void add(int shardId, SocketAddress address) {
            for (int i = 0; i < numOfReplicas; i++) {
                circle.put(replicaHash(shardId, i), address);
            }
        }

        public synchronized void remove(int shardId, SocketAddress address) {
            for (int i = 0; i < numOfReplicas; i++) {
                long hash = replicaHash(shardId, i);
                SocketAddress oldAddress = circle.get(hash);
                if (null != oldAddress && oldAddress.equals(address)) {
                    circle.remove(hash);
                }
            }
        }

        public SocketAddress get(String key, SocketAddress prevAddr) {
            long hash = hashFunction.hashString(key).asLong();
            Pair<Long, SocketAddress> pair = get(hash);
            if (null == pair) {
                return null;
            } else {
                if (pair.getLast().equals(prevAddr)) {
                    Pair<Long, SocketAddress> pair2 = get(pair.getFirst() + 1);
                    if (null == pair2) {
                        return null;
                    } else {
                        return pair2.getLast();
                    }
                } else {
                    return pair.getLast();
                }
            }
        }

        private synchronized Pair<Long, SocketAddress> get(long hash) {
            if (circle.isEmpty()) {
                return null;
            }

            if (!circle.containsKey(hash)) {
                SortedMap<Long, SocketAddress> tailMap = circle.tailMap(hash);
                hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            }
            return Pair.of(hash, circle.get(hash));
        }

        synchronized void dumpHashRing() {
            for (Map.Entry<Long, SocketAddress> entry : circle.entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
        }

    }

    protected final HashFunction hashFunction = Hashing.md5();
    protected final ConsistentHash circle;
    protected final Map<Integer, SocketAddress> shardId2Address =
            new HashMap<Integer, SocketAddress>();
    protected final Map<SocketAddress, Integer> address2ShardId =
            new HashMap<SocketAddress, Integer>();

    private ConsistentHashRoutingService(ServerSet serverSet, int numReplicas) {
        super(serverSet);
        circle = new ConsistentHash(hashFunction, numReplicas);
    }

    @Override
    public SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException {
        SocketAddress host = circle.get(key, previousAddr);
        if (null != host) {
            return host;
        }
        throw new NoBrokersAvailableException("No host found for " + key + ", previous : " + previousAddr);
    }

    @Override
    public void removeHost(SocketAddress host, Throwable reason) {
        synchronized (shardId2Address) {
            Integer shardId = address2ShardId.remove(host);
            if (null != shardId) {
                SocketAddress curHost = shardId2Address.get(shardId);
                if (null != curHost && curHost.equals(host)) {
                    shardId2Address.remove(shardId);
                }
                circle.remove(shardId, host);
                logger.info("Shard {} ({}) left due to : ",
                        new Object[] { shardId, host, reason });
            } else {
                logger.info("Node {} left due to : ", host, reason);
            }
        }
    }

    @Override
    protected synchronized void performServerSetChange(ImmutableSet<ServiceInstance> serverSet) {
        Map<Integer, SocketAddress> newMap = new HashMap<Integer, SocketAddress>();
        for (ServiceInstance serviceInstance : serverSet) {
            Endpoint endpoint = serviceInstance.getAdditionalEndpoints().get("thrift");
            SocketAddress address = new InetSocketAddress(endpoint.getHost(), endpoint.getPort());
            newMap.put(serviceInstance.getShard(), address);
        }

        Map<Integer, SocketAddress> removed;
        Map<Integer, SocketAddress> added;
        synchronized (shardId2Address) {
            MapDifference<Integer, SocketAddress> difference =
                    Maps.difference(shardId2Address, newMap);
            removed = difference.entriesOnlyOnLeft();
            added = difference.entriesOnlyOnRight();
            for (Integer shard : removed.keySet()) {
                SocketAddress host = shardId2Address.remove(shard);
                if (null != host) {
                    address2ShardId.remove(host);
                    circle.remove(shard, host);
                    logger.info("Shard {} ({}) left.", shard, host);
                }
            }
            for (Map.Entry<Integer, SocketAddress> entry : added.entrySet()) {
                shardId2Address.put(entry.getKey(), entry.getValue());
                address2ShardId.put(entry.getValue(), entry.getKey());
                circle.add(entry.getKey(), entry.getValue());
                logger.info("Shard {} ({}) joined.", entry.getKey(), entry.getValue());
            }
        }

        for (SocketAddress addr : removed.values()) {
            for (RoutingListener listener : listeners) {
                listener.onServerLeft(addr);
            }
        }

        for (SocketAddress addr : added.values()) {
            for (RoutingListener listener : listeners) {
                listener.onServerJoin(addr);
            }
        }
    }

    public static void main(String[] args) {
        ConsistentHash circle = new ConsistentHash(Hashing.md5(), 997);

        int numNodes = 40;
        int numStreams = 8000;

        for (int i = 0; i < numNodes; i++) {
            circle.add(i, new InetSocketAddress("localhost", 8000 + i));
        }

        circle.dumpHashRing();

        Map<SocketAddress, Set<String>> addr2Streams =
                new HashMap<SocketAddress, Set<String>>();
        for (int i = 0; i < numNodes; i++) {
            for (int j = 0; j < numStreams / numNodes; j++) {
                String stream = "QuantumLeapX-" + i + "-" + j;
                SocketAddress address = circle.get(stream, null);
                Set<String> streams = addr2Streams.get(address);
                if (null == streams) {
                    streams = new HashSet<String>();
                    addr2Streams.put(address, streams);
                }
                streams.add(stream);
            }
        }

        for (Map.Entry<SocketAddress, Set<String>> entry : addr2Streams.entrySet()) {
            System.out.println(entry.getKey() + " : size = " + entry.getValue().size() + ", -> " + entry.getValue());
        }

        circle.remove(0, new InetSocketAddress("localhost", 8000));
        // circle.add(0, new InetSocketAddress("localhost", 9000));
        Map<SocketAddress, Set<String>> addr2Streams2 =
                new HashMap<SocketAddress, Set<String>>();
        for (int i = 0; i < numNodes; i++) {
            for (int j = 0; j < numStreams / numNodes; j++) {
                String stream = "QuantumLeapX-" + i + "-" + j;
                SocketAddress address = circle.get(stream, null);
                Set<String> streams = addr2Streams2.get(address);
                if (null == streams) {
                    streams = new HashSet<String>();
                    addr2Streams2.put(address, streams);
                }
                streams.add(stream);
            }
        }
        diff(addr2Streams, addr2Streams2);

        for (Map.Entry<SocketAddress, Set<String>> entry : addr2Streams2.entrySet()) {
            System.out.println(entry.getKey() + " : size = " + entry.getValue().size() + ", -> " + entry.getValue());
        }
    }

    private static void diff(Map<SocketAddress, Set<String>> map1, Map<SocketAddress, Set<String>> map2) {
        Set<SocketAddress> ks1 = map1.keySet();
        Set<SocketAddress> ks2 = map2.keySet();
        Set<SocketAddress> diff1 = Sets.difference(ks1, ks2).immutableCopy();
        System.out.println("Map1 - Map2");
        for (SocketAddress addr : diff1) {
            Set<String> streams = map1.get(addr);
            System.out.println(addr + " : size = " + streams.size() + ", -> " + streams);
        }
        Set<SocketAddress> diff2 = Sets.difference(ks2, ks1).immutableCopy();
        System.out.println("Map2 - Map1");
        for (SocketAddress addr : diff2) {
            Set<String> streams = map2.get(addr);
            System.out.println(addr + " : size = " + streams.size() + ", -> " + streams);
        }
        Set<SocketAddress> diff3 = Sets.intersection(ks1, ks2).immutableCopy();
        for (SocketAddress addr : diff3) {
            Set<String> ss1 = map1.get(addr);
            Set<String> ss2 = map2.get(addr);
            System.out.println(addr + " : diff " + Sets.symmetricDifference(ss1, ss2));
        }
    }

}
